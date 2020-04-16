# Example of simple application 
This module contains example of Flink application (SmoothingPrices).

## Application Description

https://wiki.riskfocus.com/pages/viewpage.action?pageId=147395358

## Run example of pipeline
 1. Build jar using maven from the root directory
    ```bash
    $ mvn clean package
    ```
 2. Start the Kafka cluster
    ```bash
    $ cd docker
    $ docker-compose up -d
    ```    
 3. Make sure that all containers up and running `docker ps`
 4. Wait for `initializer` to create topics, this will take a ~ 2 min.
    ```bash
    docker logs -f initializer
    ...
    Created topic optionsPricesLive.
    Created topic ycInputsLive.
    Created topic interestRatesSnapshot.
    Created topic smoothingInputsLatest.
    ```    
 3. Now you can run application locally in IntelliJ IDEA.
    1. You have to specify as a `Main class:` in Run Configuration `com.riskfocus.flink.example.pipeline.SmoothingPricesJob`.
    2. Then check setting: `Include dependencies with "Provided" scope`
    3. Also you need to specify parameter `-local.dev true` in `Program arguments:` field.

