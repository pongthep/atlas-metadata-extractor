# Apache Atlas Metadata Extractor
Metadata extractor to support using apache atlas

![Alt text](docs/images/architecture_flow.png?raw=true)

### using step by step
1. create connection by using facetories/connection_factory.py to connect datasource
2. create extractor by using facetories/extractor_factory.py to extract metadata we need from datasource
3. create builder by using facetories/builder_factory.py to convert metadata that we get from extractor into our format
4. choose publisher that you would like to send this metadata to. Mainly we use publisher/atlas

## contact
mr.pongthep@gmail.com