import streamlit as st
import requests
import base64
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

api = "<ENTER URL OF THE API GATEWAY HERE>/opensearch-lambda?q={query_text}&numMovies={num_movies}&numRecs={num_recs}"

auth = BotoAWSRequestsAuth(
    aws_host=api.split("/")[2],
    aws_region=api.split('.')[2],
    aws_service="execute-api")

def get_movies():
    result = requests.get(
        api.format(
            query_text=search_text, num_movies=num_movies, num_recs=recs_per_movie
        ),auth=auth
    ).json()
    print(result)
    st.title("Search Results")
    for idx, search_result in enumerate(result["results"]):

        st.markdown(
            f"## [{search_result['result']}](https://www.imdb.com/title/{search_result['ttId']}/)"
        )
        st.markdown(
            f"Movies related to { search_result['result']} from our catalog that we think you will like"
        )
        cols = st.columns(recs_per_movie)
        # print(search_result['rec_poster'],recs_per_movie)
        for i, (poster, title, ttid) in enumerate(
            zip(
                search_result["rec_poster"][1:],
                search_result["recs"][1:],
                search_result["recs_id"][1:],
            )
        ):
            # print(ttid)
            imdb_url = f"https://www.imdb.com/title/{ttid}/"
            # print(imdb_url)
            try:
                image_html = get_img_with_href(poster, imdb_url)
                # print(image_html)
                cols[i].markdown(image_html, unsafe_allow_html=True)
            except requests.exceptions.MissingSchema as e:
                cols[i].markdown("")
            cols[i].markdown(f"[{title}]({imdb_url})")
    return result


@st.cache(allow_output_mutation=True)
def get_as_base64(url):
    return base64.b64encode(requests.get(url).content).decode()


@st.cache(allow_output_mutation=True)
def get_img_with_href(img_url, target_url):
    html_code = f"""
        <a href="{target_url}">
            <img src="{img_url}" height="200"/>
        </a>"""
    # print(img_url, target_url)
    return html_code


if __name__ == "__main__":
    st.sidebar.image(
        "https://ia.media-imdb.com/images/M/MV5BMTc5NzU4OTU0N15BMl5BcG5nXkFtZTgwNTY0NjQ2OTE@._V1_.png"
    )
    search_text = st.sidebar.text_input(
        "Please enter search text to find movies and recommendations"
    )
    num_movies = st.sidebar.slider(
        "Number of search hits", min_value=0, max_value=5, value=1
    )
    recs_per_movie = st.sidebar.slider(
        "Number of recommendations per hit", min_value=0, max_value=10, value=5
    )
    if st.sidebar.button("Find"):
        resp = get_movies()
