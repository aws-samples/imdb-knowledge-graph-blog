import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, flatten, md5, crc32, format_string, regexp_replace
from pyspark.sql.functions import concat, col, lit, isnan, when, count, udf,element_at
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types     import StringType, BooleanType, IntegerType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'output_bucket_path',
                           'raw_data_path'])

sc = SparkContext()
sc._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

output_bucket_path = args['output_bucket_path']
raw_data_path = args['raw_data_path']

def filterMovies(titles, names, prefix = f"{output_bucket_path}/parquet"):

    mdf      = titles.filter(titles.titleType == 'movie')

    mdf.coalesce(1).write.mode("overwrite").parquet(f"{prefix}/movies.parquet")

    cast    = mdf.select(explode("principalCastMembers").alias("cast")).select("cast.nameId")
    crew    = mdf.select(explode("principalCrewMembers").alias("crew")).select("crew.nameId")
    credits = cast.join(crew, "nameId", how = "outer").distinct()

    pdf      = names.join(credits, "nameId", how = "inner")

    pdf.coalesce(1).write.mode("overwrite").parquet(f"{prefix}/people.parquet")

    print(f"# of Movie Titles : {mdf.count():>7}")
    print(f"# of Movie People : {pdf.count():>7}")
    return mdf,pdf


def dumpMovie(tf, prefix = f"{output_bucket_path}/graph"):
    @udf
    def fix(string):
        return str(string).replace('"', "''") 

    opening_df = spark.read.json(f'{raw_data_path}/boxoffice_title_opening_weekends_v1.jsonl.gz')
    budgets_df = spark.read.json(f'{raw_data_path}/boxoffice_title_budgets_v1.jsonl.gz')
    grosses_df = spark.read.json(f'{raw_data_path}/boxoffice_title_grosses_v1.jsonl.gz')

    grosses_in = grosses_df.filter(col("area") == "XNDOM").select("titleId", col("grossToDate").alias("grossInternational").cast("long"))
    grosses_ww = grosses_df.filter(col("area") == "XWW"  ).select("titleId", col("grossToDate").alias("grossWorldwide").cast("int"))
    grosses_us = grosses_df.filter(col("area") == "XDOM" ).select("titleId", col("grossToDate").alias("grossDomestic").cast("int"))
    
    budgets_pr = budgets_df.filter(col("budgetItemType") == "production")\
                           .select("titleId", col("amount").alias("budgetProduction").cast("int"))

    gf = (grosses_ww.join(grosses_in, on = "titleId", how = "outer")
                   .join(grosses_us, on = "titleId", how = "outer"))

    bf = budgets_pr
    
    tf = tf.select("titleId","year",
                   col("image.url").alias("imageUrl"),
                   element_at("productionStatus.status", -1).alias("productionStatus"),
                   col("imdbRating.rating").alias("rating"),
                   col("imdbRating.numberOfVotes").alias("votes"),
                   fix("originalTitle").alias("originalTitle"))
    
    tf = tf.join(gf, on = "titleId", how = "left")
    tf = tf.join(bf, on = "titleId", how = "left")
    
    na = {'poster:String'           : 'unavailable',
          'rating:Float'            : -1.0,
          'votes:Int'               : -1,
          'gross_international:Long' : -1,
          'gross_worldwide:Int'     : -1,
          'gross_domestic:Int'      : -1,
          'budget_production:Int'   : -1,
          'year:Int'                : -1,
          'status:String'           : 'unknown'}

    """
    nodes : ~id, ~label, <property>:<type>, ...
    """
    nodes = tf.select(
        col("titleId"           ).alias("~id"),
        lit("movie"             ).alias("~label"),
        col("originalTitle"     ).alias("name:String"),
        col("imageUrl"          ).alias("poster:String"),
        col("year"              ).alias("year:Int"),
        col("productionStatus"  ).alias("status:String"),
        col("rating"            ).alias("rating:Float"),
        col("votes"             ).alias("votes:Int"),
        col("grossInternational").alias("gross_international:Long"),
        col("grossWorldwide"    ).alias("gross_worldwide:Int"),
        col("grossDomestic"     ).alias("gross_domestic:Int"),
        col("budgetProduction"  ).alias("budget_production:Int")).distinct().na.fill(na)

    print("node count is", nodes.count())

    nodes.coalesce(1).write.mode("overwrite").csv(f"{prefix}/nodes/movie", header = True)

    #assert nodes.count() == 602895


def dumpGenre(tf, prefix = f"{output_bucket_path}/graph"):
    gf = tf.select(tf.titleId, explode(tf.genres).alias("genre"))
    gf = gf.select(col("titleId"),
                   format_string("gn%010d", crc32("genre")).alias("genreId"),
                   col("genre"))
    """
    edges : ~id, ~from, ~to, ~label, <property>:<type>, ...
    """
    edges = gf.select(
        format_string("%s-genre-%s", "titleId", "genreId").alias("~id"),
        col("titleId").alias("~from"),
        col("genreId").alias("~to"),
        lit("is-genre").alias("~label"))
    """
    nodes : ~id, ~label, <property>:<type>, ...
    """
    nodes = gf.select(
        col("genreId").alias("~id"),
        lit("genre").alias("~label"),
        col("genre").alias("name:String")).distinct()

    print("edge count is", edges.count())
    print("node count is", nodes.count())

    edges.coalesce(1).write.mode("overwrite").csv(f"{prefix}/edges/movie-genre", header = True)
    nodes.coalesce(1).write.mode("overwrite").csv(f"{prefix}/nodes/genre", header = True)

def dumpKeyword(tf, prefix = f"{output_bucket_path}/graph"):
    kf = tf.select("titleId", explode("keywordsV2").alias("v2"))
    kf = kf.select("titleId",
                   col("v2.category").alias("category"),
                   col("v2.keyword").alias("keyword"))
    kf = kf.na.fill("other", subset=["category"])
    kf = kf.select(
        "titleId",
        format_string("kn%010d", crc32("keyword")).alias("keywordId"),
        "keyword",
        "category")
    """
    edges : ~id, ~from, ~to, ~label, <property>:<type>, ...
    """
    edges = kf.select(
        format_string("%s-keyword-%s", "titleId", "keywordId").alias("~id"),
        col("titleId").alias("~from"),
        col("keywordId").alias("~to"),
        format_string("described-by-%s-keyword", "category").alias("~label"))
    """
    nodes : ~id, ~label, <property>:<type>, ...
    """
    nodes = kf.select(
        col("keywordId").alias("~id"),
        lit("keyword").alias("~label"),
        regexp_replace("keyword", '["]', "").alias("name:String"),
        col("category").alias("keyword_type:String")).distinct()

    print("edge count is", edges.count())
    print("node count is", nodes.count())

    edges.coalesce(1).write.mode("overwrite").csv(f"{prefix}/edges/movie-keyword", header = True)
    nodes.coalesce(1).write.mode("overwrite").csv(f"{prefix}/nodes/keyword", header = True)

def dumpContributor(tf,ndf, prefix = f"{output_bucket_path}/graph"):
    ndf = ndf.select("nameId")
    
    cats = ['director', 'producer', 'composer', 'writer', 'editor', 'cinematographer', 'production_designer']

    cast = tf.select('titleId', explode('principalCastMembers').alias('cast')).select('titleId','cast.nameId', 'cast.category')
    crew = tf.select('titleId', explode('principalCrewMembers').alias('crew')).select('titleId','crew.nameId', 'crew.category').where(col('category').isin(cats))
    
    cast = cast.join(ndf,"nameId", how = "inner")
    crew = crew.join(ndf,"nameId", how = "inner")

    """
    edges : ~id, ~from, ~to, ~label, <property>:<type>, ...
    """

    edges = cast.select(
        format_string("%s-cast-%s", "titleId", "nameId").alias("~id"),
        col("titleId").alias("~from"),
        col("nameId").alias("~to"),
        format_string("casted-by-%s", "category").alias("~label"))

    edges = edges.union(crew.select(
        format_string("%s-crew-%s", "titleId", "nameId").alias("~id"),
        col("titleId").alias("~from"),
        col("nameId").alias("~to"),
        format_string("crewed-by-%s", "category").alias("~label"))).distinct()

    edges.coalesce(1).write.mode("overwrite").csv(f"{prefix}/edges/person-title", header = True)

    #edges.show()

    print("edge count is", edges.count())

def dumpPerson(ndf, tdf, prefix = f"{output_bucket_path}/graph"):
    # Node
    @udf(returnType=BooleanType())
    def oscarNominee(awards):
        return any(a.awardName == "Oscar" for a in awards or [])

    @udf(returnType=BooleanType())
    def oscarWinner(awards):
        return any(a.awardName == "Oscar" and a.winner for a in awards or [])
        
    @udf
    def fix(string):
        return str(string).replace('"', "''") 
        
    pf = ndf.select(col("nameId"),
                   fix("name").alias("name"),
                   oscarNominee("awards").alias("oscarNominee"),
                   oscarWinner("awards").alias("oscarWinner"))
    """
    nodes : ~id, ~label, <property>:<type>, ...
    """
    nodes = pf.select(
        col("nameId"              ).alias("~id"),
        lit("person"              ).alias("~label"),
        col("name"                ).alias("name:String"),
        col("oscarNominee"        ).alias("oscar_nominee:Bool"),
        col("oscarWinner"         ).alias("oscar_winner:Bool"),
    ).distinct()

    nodes.printSchema()
  # nodes.show()

    print("node count is", nodes.count())

    nodes.coalesce(1).write.mode("overwrite").csv(f"{prefix}/nodes/person", header = True, quoteAll = True)


def dumpRating(tdf, prefix = f"{output_bucket_path}/graph"):
    rate = tdf.select('imdbRating.rating').dropDuplicates()\
            .withColumn("~id",concat(lit("rate"),(col("rating")*10).cast('int')))\
            .withColumn("~label",lit("rating"))\
            .withColumnRenamed("rating","rating:Float")\
            .na.drop()
    r = rate.select('~id','~label','rating:Float')
    #r.show(5)
    print(f"Node: IMDB Rating: {rate.count()}")
    r.coalesce(1).write.csv(f"{prefix}/nodes/rating/",header=True)
    n = tdf.select('titleId','imdbRating.rating').dropDuplicates()
    n1 = n.withColumn("~id",concat(lit("eTTRt-"),(col("rating")*10).cast('int'),lit("-"),col('titleId')))\
          .withColumnRenamed("titleId","~from")\
          .withColumn("~to",concat(lit("rate"),(col("rating")*10).cast('int')))\
          .withColumn("~label",lit("has_rating"))
    n1=n1.na.drop(how='any')
    #n1.show(5)
    print(f"Edge: IMDB Rating: {n1.count()}")
    n1 = n1.select('~id','~from','~to','~label')
    n1.coalesce(1).write.csv(f"{prefix}/edges/has_rating/",header=True)

def dumpAwards(ndf,tdf,prefix = f"{output_bucket_path}/graph"):
    # Node
    awards_title = tdf.select("titleId",explode("awards").alias("awards")).select('titleId','awards.*')
    awards_name = ndf.select("nameId",explode("awards").alias("awards")).select('nameId','awards.*')
    awards = awards_title.select('event').union(awards_name.select('event'))
    awards_event = awards.select("event")\
                        .withColumn("~id",crc32(col("event")))\
                        .withColumn("~label",lit("award_event"))\
                        .withColumn("event",regexp_replace(col("event"),'"',"'"))\
                        .withColumnRenamed("event","event:String")\
                        .dropDuplicates()
    ae = awards_event.select("~id","~label","event:String")
    print(f"Node: Award: {ae.count()}")
    #ae.show(5,False)
    ae.coalesce(1).write.csv(f"{prefix}/nodes/award_event/",header=True)
    # Edges: winner, nominations
    at = awards_title.select(concat(col("awardNominationId"),lit("-"),col("titleId"),lit("-"),col("year")).alias("~id"),col("titleId").alias("~from"),"event","winner","year")\
        .withColumn("event",crc32(col("event")))\
        .withColumnRenamed("event","~to")\
        .withColumnRenamed("year","year:Int")\
        .withColumn("~label",lit("nominated_for")).distinct()
    at_nom = at.filter("winner == 0")
    at_won = at.filter("winner == 1")
    at_nom = at_nom.select("~id","~from","~to","~label","year:Int")
    print(f"Edge: Award nominated: {at_nom.count()}")
    #at_nom.show(5,False)
    at_nom.coalesce(1).write.csv(f"{prefix}/edges/has_nomination/",header=True)
    at_won= at_won.withColumn("~id",concat(lit("aw"),col("~id")))\
                  .withColumn("~label",lit("has_won"))
    at_won = at_won.select("~id","~from","~to","~label","year:Int")
    print(f"Edge: Award won: {at_won.count()}")
    #at_won.show(5,False)
    at_won.coalesce(1).write.csv(f"{prefix}/edges/has_won/",header=True)
    
def dumpPlace(tf, prefix=f"{output_bucket_path}/graph"):

    from pyspark.sql.functions import explode, flatten, udf, crc32, format_string, regexp_replace, concat, col, lit, isnan, when, count

    @udf
    def fix(string):
        return str(string).replace('"', "''")
    
    lf = tf.select("titleId", explode("locations").alias("location"))
    lf = lf.select("titleId", col("location.place").alias("place"))
    lf = lf.select("titleId", format_string("pl%010d", crc32("place")).alias("placeId"), col("place"))

    """
    edges : ~id, ~from, ~to, ~label, <property>:<type>, ...
    """
    edges = lf.select(
        format_string("%s-place-%s", "titleId", "placeId").alias("~id"),
        col("titleId").alias("~from"),
        col("placeId").alias("~to"),
        lit("filmed-at").alias("~label"))
    """
    nodes : ~id, ~label, <property>:<type>, ...
    """
    nodes = lf.select(
        col("placeId").alias("~id"),
        lit("place").alias("~label"),
        fix("place").alias("name:String")).distinct()

    print("edge count is", edges.count())
    print("node count is", nodes.count())

    edges.coalesce(1).write.mode("overwrite").csv(f"{prefix}/edges/movie-place", header = True, quoteAll = True)
    nodes.coalesce(1).write.mode("overwrite").csv(f"{prefix}/nodes/place",       header = True, quoteAll = True)


title_df= spark.read.json(f"{raw_data_path}/title_essential_v1_complete.jsonl.gz")
name_df = spark.read.json(f"{raw_data_path}/name_essential_v1_complete.jsonl.gz")

movie_df, people_df = filterMovies(title_df,name_df)
dumpMovie(movie_df)
dumpGenre(movie_df)
dumpKeyword(movie_df)
dumpContributor(movie_df,people_df)
dumpPerson(people_df,movie_df)
dumpRating(movie_df)
dumpAwards(people_df,movie_df)
dumpPlace(movie_df)
