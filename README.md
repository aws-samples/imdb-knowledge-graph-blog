# AWS Blogpost Series - Powering Recommendation and Search using IMDb Knowledge Graph

This repo contains supporting code for the AWS blogpost series - "Powering Recommendation and Search using IMDb Knowledge Graph".
This three-part series demonstrates how to use graph neural networks (GNNs) and [Amazon Neptune](https://aws.amazon.com/neptune/) to generate movie recommendations and search application using the [IMDb and Box Office Mojo Movies/TV/OTT](https://aws.amazon.com/marketplace/pp/prodview-l4lwqeffv72uo) licensable data package. 

This package provides a wide range of entertainment metadata, including over 1 billion user ratings; credits for more than 11 million cast and crew members; 9 million movie, TV, and entertainment titles; and global box office reporting data from more than 60 countries. Many AWS media and entertainment customers license IMDb data through [AWS Data Exchange](https://aws.amazon.com/marketplace/search/results?searchTerms=imdb+sample) to improve content discovery and increase customer engagement and retention.

This blog consists of 3 parts
1. Graph creation -  where we will create graph nodes and edges files from the [IMDb and Box Office Mojo Movies/TV/OTT dataset](https://aws.amazon.com/marketplace/pp/prodview-l4lwqeffv72uo).
2. Embedding generation using [NeptuneML](https://aws.amazon.com/neptune/machine-learning/) - where we will load the graph file into Amazon [Neptune](https://aws.amazon.com/neptune/), process them and train a GNN model using [NeptuneML](https://aws.amazon.com/neptune/machine-learning/).
3. Out of catalog search application - where we explain what out-of-catalog search, create [Amazon OpenSearch](https://aws.amazon.com/what-is/opensearch/) cluster & index, and simulate an example with a local streamlit app for illustration.

A detailed walkthrough of how to run this repo is discussed in the blogposts.

## License
This sample code is licensed under the MIT-0 License. See the LICENSE file.

## Note
For blog 3, this blog has supressed some CDK-NAG issues that we found. For production, please make sure you solve these issues.