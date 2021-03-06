// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.async;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncContainerResponse;
import com.azure.cosmos.CosmosContainerProperties;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosAsyncDatabaseResponse;
import com.azure.cosmos.CosmosAsyncItem;
import com.azure.cosmos.CosmosItemProperties;
import com.azure.cosmos.CosmosItemRequestOptions;
import com.azure.cosmos.FeedOptions;
import com.azure.cosmos.FeedResponse;
import com.azure.cosmos.Resource;
import com.azure.cosmos.sample.common.AccountSettings;
import com.azure.cosmos.sample.common.Families;
import com.azure.cosmos.sample.common.Family;
import com.google.common.collect.Lists;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;


import java.util.concurrent.CountDownLatch;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.List;

public class AsyncMain {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    public void close() {
        client.close();
    }

    /**
     * Run a Hello CosmosDB console application.
     *
     * @param args command line args.
     */
    //  <Main>
    public static void main(String[] args) {
        AsyncMain p = new AsyncMain();

        try {
            p.getStartedDemo();
            System.out.println("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            System.out.println("Closing the client");
            p.close();
        }
        System.exit(0);
    }

    //  </Main>

    private void getStartedDemo() throws Exception {
        System.out.println("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        ConnectionPolicy defaultPolicy = ConnectionPolicy.getDefaultPolicy();
        //  Setting the preferred location to Cosmos DB Account region
        //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the application
        defaultPolicy.setPreferredLocations(Lists.newArrayList("West US"));

        //  Create async client
        //  <CreateAsyncClient>
        client = new CosmosClientBuilder()
            .setEndpoint(AccountSettings.HOST)
            .setKey(AccountSettings.MASTER_KEY)
            .setConnectionPolicy(defaultPolicy)
            .setConsistencyLevel(ConsistencyLevel.EVENTUAL)
            .buildAsyncClient();

        //  </CreateAsyncClient>

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        Family andersenFamilyItem=Families.getAndersenFamilyItem();
        Family wakefieldFamilyItem=Families.getWakefieldFamilyItem();
        Family johnsonFamilyItem=Families.getJohnsonFamilyItem();
        Family smithFamilyItem=Families.getSmithFamilyItem();

        //  Setup family items to create
        Flux<Family> familiesToCreate = Flux.just(andersenFamilyItem,
                                            wakefieldFamilyItem,
                                            johnsonFamilyItem,
                                            smithFamilyItem);

        createFamilies(familiesToCreate);

        familiesToCreate = Flux.just(andersenFamilyItem,
                                wakefieldFamilyItem,
                                johnsonFamilyItem,
                                smithFamilyItem);

        System.out.println("Reading items.");
        readItems(familiesToCreate);

        System.out.println("Querying items.");
        queryItems();
    }

    private void createDatabaseIfNotExists() throws Exception {
        System.out.println("Create database " + databaseName + " if not exists.");

        //  Create database if not exists
        //  <CreateDatabaseIfNotExists>
        Mono<CosmosAsyncDatabaseResponse> databaseIfNotExists = client.createDatabaseIfNotExists(databaseName);
        databaseIfNotExists.flatMap(databaseResponse -> {
            database = databaseResponse.getDatabase();
            System.out.println("Checking database " + database.getId() + " completed!\n");
            return Mono.empty();
        }).block();
        //  </CreateDatabaseIfNotExists>
    }

    private void createContainerIfNotExists() throws Exception {
        System.out.println("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        //  <CreateContainerIfNotExists>

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/lastName");
        Mono<CosmosAsyncContainerResponse> containerIfNotExists = database.createContainerIfNotExists(containerProperties, 400);
        
        //  Create container with 400 RU/s
        containerIfNotExists.flatMap(containerResponse -> {
            container = containerResponse.getContainer();
            System.out.println("Checking container " + container.getId() + " completed!\n");
            return Mono.empty();
        }).block();

        //  </CreateContainerIfNotExists>
    }

    private void createFamilies(Flux<Family> families) throws Exception {

        //  <CreateItem>

        final CountDownLatch completionLatch = new CountDownLatch(1);

        //  Combine multiple item inserts, associated success println's, and a final aggregate stats println into one Reactive stream.
        families.flatMap(family -> {
                CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions(family.getLastName());
                return container.createItem(family, cosmosItemRequestOptions);
            }) //Flux of item request responses
            .flatMap(itemResponse -> {
                System.out.println(String.format("Created item with request charge of %.2f within" +
                    " duration %s",
                    itemResponse.getRequestCharge(), itemResponse.getRequestLatency()));
                System.out.println(String.format("Item ID: %s\n", itemResponse.getItem().getId()));
                return Mono.just(itemResponse.getRequestCharge());
            }) //Flux of request charges
            .reduce(0.0, 
                (charge_n,charge_nplus1) -> charge_n + charge_nplus1
            ) //Mono of total charge - there will be only one item in this stream            
            .subscribe(charge -> { 
                System.out.println(String.format("Created items with total request charge of %.2f\n",
                charge));         
            }, 
                err -> {
                    if (err instanceof CosmosClientException) {
                        //Client-specific errors
                        CosmosClientException cerr = (CosmosClientException)err;
                        cerr.printStackTrace();
                        System.err.println(String.format("Read Item failed with %s\n", cerr));
                    } else {
                        //General errors
                        err.printStackTrace();
                    }

                    completionLatch.countDown();                    
                }, 
                () -> {completionLatch.countDown();}
        ); //Preserve the total charge and print aggregate charge/item count stats.

        try {
            completionLatch.await();            
        } catch (InterruptedException err) {
            throw new AssertionError("Unexpected Interruption",err);
        }

        //  </CreateItem>            
    }

    private void readItems(Flux<Family> familiesToCreate) {
        //  Using partition key for point read scenarios.
        //  This will help fast look up of items because of partition key
        //  <ReadItem>

        final CountDownLatch completionLatch = new CountDownLatch(1);
        
        familiesToCreate.flatMap(family -> {
                            CosmosAsyncItem item = container.getItem(family.getId(), family.getLastName());
                            return item.read(new CosmosItemRequestOptions(family.getLastName()));
                        })
                        .subscribe(
                            itemResponse -> {
                                double requestCharge = itemResponse.getRequestCharge();
                                Duration requestLatency = itemResponse.getRequestLatency();
                                System.out.println(String.format("Item successfully read with id %s with a charge of %.2f and within duration %s",
                                    itemResponse.getItem().getId(), requestCharge, requestLatency));
                            },
                            err -> {
                                if (err instanceof CosmosClientException) {
                                    //Client-specific errors
                                    CosmosClientException cerr = (CosmosClientException)err;
                                    cerr.printStackTrace();
                                    System.err.println(String.format("Read Item failed with %s\n", cerr));
                                } else {
                                    //General errors
                                    err.printStackTrace();
                                }

                                completionLatch.countDown();
                            },
                            () -> {completionLatch.countDown();}
        );

        try {
            completionLatch.await();             
        } catch (InterruptedException err) {
            throw new AssertionError("Unexpected Interruption",err);
        }

        //  </ReadItem>
    }

    private void queryItems() {
        //  <QueryItems>
        // Set some common query options

        FeedOptions queryOptions = new FeedOptions();
        queryOptions.maxItemCount(10);
        queryOptions.setEnableCrossPartitionQuery(true);
        //  Set populate query metrics to get metrics around query executions
        queryOptions.populateQueryMetrics(true);

        Flux<FeedResponse<CosmosItemProperties>> pagedFluxResponse = container.queryItems(
            //"SELECT * FROM Family WHERE Family.lastName IN ('Andersen', 'Wakefield', 'Johnson')", queryOptions);
            "SELECT * FROM Family WHERE Family.lastName IN ('Andersen', 'Wakefield', 'Johnson')", queryOptions);

        final CountDownLatch completionLatch = new CountDownLatch(1);

        pagedFluxResponse.subscribe(
            fluxResponse -> {
                System.out.println("Got a page of query result with " +
                    fluxResponse.getResults().size() + " items(s)"
                    + " and request charge of " + fluxResponse.getRequestCharge());

                System.out.println("Item Ids " + fluxResponse
                    .getResults()
                    .stream()
                    .map(Resource::getId)
                    .collect(Collectors.toList()));
            },
            err -> {
                if (err instanceof CosmosClientException) {
                    //Client-specific errors
                    CosmosClientException cerr = (CosmosClientException)err;
                    cerr.printStackTrace();
                    System.err.println(String.format("Read Item failed with %s\n", cerr));
                } else {
                    //General errors
                    err.printStackTrace();
                }

                completionLatch.countDown();
            },
            () -> {completionLatch.countDown();}
        );

        try {
            completionLatch.await();             
        } catch (InterruptedException err) {
            throw new AssertionError("Unexpected Interruption",err);
        }

        // </QueryItems>
    }
}
