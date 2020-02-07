// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.async;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
//import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosClientException;
//import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncContainerResponse;
import com.azure.cosmos.CosmosContainerProperties;
//import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosAsyncDatabaseResponse;
//import com.azure.cosmos.CosmosItem;
import com.azure.cosmos.CosmosAsyncItem;
import com.azure.cosmos.CosmosAsyncItemResponse;
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
import reactor.core.publisher.MonoOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.FluxProcessor;
import reactor.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

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

        createDatabaseIfNotExists().subscribe(s -> {}, err -> {}, () -> {});
        createContainerIfNotExists().subscribe(s -> {}, err -> {}, () -> {});

        //  Setup family items to create
        Flux<Family> familiesToCreate = Flux.just(Families.getAndersenFamilyItem(),
                                                  Families.getWakefieldFamilyItem(),
                                                  Families.getJohnsonFamilyItem(),
                                                  Families.getSmithFamilyItem());

        createFamilies(familiesToCreate);

        System.out.println("Reading items.");
        readItems(familiesToCreate);

        System.out.println("Querying items.");
        queryItems();
    }

    private Mono<CosmosAsyncDatabase> createDatabaseIfNotExists() throws Exception {
        System.out.println("Create database " + databaseName + " if not exists.");

        //  Create database if not exists
        //  <CreateDatabaseIfNotExists>
        Mono<CosmosAsyncDatabaseResponse> databaseIfNotExists = client.createDatabaseIfNotExists(databaseName);
        return databaseIfNotExists.flatMap(x -> {
            database = x.getDatabase();
            System.out.println("Checking database " + database.getId() + " completed!\n");
            return Mono.just(database);
        });
        //  </CreateDatabaseIfNotExists>
    }

    private Mono<CosmosAsyncContainer> createContainerIfNotExists() throws Exception {
        System.out.println("Create container " + containerName + " if not exists.");

        //  Create container if not exists
        //  <CreateContainerIfNotExists>

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/lastName");
        Mono<CosmosAsyncContainerResponse> containerIfNotExists = database.createContainerIfNotExists(containerProperties, 400);
        
        //  Create container with 400 RU/s
        return containerIfNotExists.flatMap(x -> {
            container = x.getContainer();
            System.out.println("Checking container " + container.getId() + " completed!\n");
            return Mono.just(container);
        });

        //  </CreateContainerIfNotExists>
    }

    private void createFamilies(Flux<Family> families) throws Exception {

        double total_charge=0.0;

        //  <CreateItem>
        //  Combine multiple item inserts, associated success println's, and a final aggregate stats println into one Reactive stream.
        families.flatMap(fm -> {
                CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions(fm.getLastName());
                return container.createItem(fm, cosmosItemRequestOptions);
            }) //Flux of item request responses
            .flatMap(itr -> {
                System.out.println(String.format("Created item with request charge of %.2f within" +
                    " duration %s",
                    itr.getRequestCharge(), itr.getRequestLatency()));
                return Mono.just(itr.getRequestCharge());
            }) //Flux of request charges
            .reduce(0.0, 
                (chg1,chg2) -> chg1 + chg2
            ) //Mono of total charge - there will be only one item in this stream
            .subscribe(s -> {
                    total_charge=s;
                }, 
                err -> {}, 
                () -> {
                    System.out.println(String.format("Created %d items with total request " +
                        "charge of %.2f",
                        families.count(),
                        total_charge));                    
            }); //Subscriber preserves the total charge and prints aggregate charge/item count stats on complete.
    }

    private void readItems(ArrayList<Family> familiesToCreate) {
        //  Using partition key for point read scenarios.
        //  This will help fast look up of items because of partition key
        familiesToCreate.forEach(family -> {
            //  <ReadItem>
            CosmosAsyncItem item = container.getItem(family.getId(), family.getLastName());
            try {
                CosmosAsyncItemResponse read = item.read(new CosmosItemRequestOptions(family.getLastName()));
                double requestCharge = read.getRequestCharge();
                Duration requestLatency = read.getRequestLatency();
                System.out.println(String.format("Item successfully read with id %s with a charge of %.2f and within duration %s",
                    read.getItem().getId(), requestCharge, requestLatency));
            } catch (CosmosClientException e) {
                e.printStackTrace();
                System.err.println(String.format("Read Item failed with %s", e));
            }
            //  </ReadItem>
        });
    }

    private void queryItems() {
        //  <QueryItems>
        // Set some common query options
        FeedOptions queryOptions = new FeedOptions();
        queryOptions.maxItemCount(10);
        queryOptions.setEnableCrossPartitionQuery(true);
        //  Set populate query metrics to get metrics around query executions
        queryOptions.populateQueryMetrics(true);

        Iterator<FeedResponse<CosmosItemProperties>> feedResponseIterator = container.queryItems(
            "SELECT * FROM Family WHERE Family.lastName IN ('Andersen', 'Wakefield', 'Johnson')", queryOptions);

        feedResponseIterator.forEachRemaining(cosmosItemPropertiesFeedResponse -> {
            System.out.println("Got a page of query result with " +
                cosmosItemPropertiesFeedResponse.getResults().size() + " items(s)"
                + " and request charge of " + cosmosItemPropertiesFeedResponse.getRequestCharge());

            System.out.println("Item Ids " + cosmosItemPropertiesFeedResponse
                .getResults()
                .stream()
                .map(Resource::getId)
                .collect(Collectors.toList()));
        });
        //  </QueryItems>
    }
}
