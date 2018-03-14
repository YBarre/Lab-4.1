// Console App example showing how to do basic Azure table operations - QM
// Code adapted from https://docs.microsoft.com/en-us/azure/storage/storage-dotnet-how-to-use-tables 
// View the table contents with VS cloud exporer or Storage Explorer after running.
// The table will need to be deleted before rerunning - how can you improve this?

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Configuration;

namespace TestTables
{
    public class Program
    {
        static void Main(string[] args)
        {
            // Use a single partition - OK for us but not for real for scalability!
            const String partitionName = "My_Peoples_Partition";

            try
            {
                // Azure tables are just another form of storage so this is the same as before with blobs and queues...
                // As before you will have to set the connection string in App.config
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.ConnectionStrings["AzureWebJobsStorage"].ToString());

                CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

                // Give the table a name and create it if it does not already exist.
                CloudTable table = tableClient.GetTableReference("people");
                table.CreateIfNotExists();

                #region CREATE
                CustomerEntity customer1 = new CustomerEntity(partitionName, "Walter");
                customer1.Email = "Walter@contoso.com";
                customer1.PhoneNumber = "425-555-0101";

                // Create the TableOperation that inserts the customer entity.
                var insertOperation = TableOperation.Insert(customer1);

                // Execute the insert operation.
                table.Execute(insertOperation);

                // Create the batch operation.
                TableBatchOperation batchOperation = new TableBatchOperation();

                // Create a customer entity and add it to the table.
                CustomerEntity customer2 = new CustomerEntity(partitionName, "Jeff");
                customer2.Email = "Jeff@contoso.com";
                customer2.PhoneNumber = "425-555-0104";

                // Create another customer entity and add it to the table.
                CustomerEntity customer3 = new CustomerEntity(partitionName, "Ben");
                customer3.Email = "Ben@contoso.com";
                customer3.PhoneNumber = "425-555-0102";

                // Add both customer entities to the batch insert operation.
                batchOperation.Insert(customer2);
                batchOperation.Insert(customer3);

                // Execute the batch operation.
                table.ExecuteBatch(batchOperation);
                #endregion


                #region READ
                // Construct the query operation for all customer entities in partition called "My_Peoples_Partition".
                TableQuery<CustomerEntity> query2 = new TableQuery<CustomerEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionName));
                List<CustomerEntity> entityList2 = new List<CustomerEntity>(table.ExecuteQuery(query2));

                // Print the fields for each customer.
                foreach (CustomerEntity entity2 in entityList2)
                {
                    Console.WriteLine("{0}; {1}; {2}; {3}", entity2.PartitionKey, entity2.RowKey, entity2.Email, entity2.PhoneNumber);
                }
                Console.WriteLine();

                // Create a retrieve operation that takes a customer entity.
                TableOperation retrieveOperation2 = TableOperation.Retrieve<CustomerEntity>(partitionName, "Ben");

                // Execute the retrieve operation.
                TableResult retrievedResult2 = table.Execute(retrieveOperation2);

                // Print the phone number of the result.
                if (retrievedResult2.Result != null)
                {
                    Console.WriteLine("Ben's phone number: " + ((CustomerEntity)retrievedResult2.Result).PhoneNumber); // note the cast
                }
                else
                {
                    Console.WriteLine("Ben's phone number could not be retrieved.");
                }
                #endregion


                #region UPDATE
                // Create a retrieve operation that takes a customer entity.
                TableOperation retrieveOperation3 = TableOperation.Retrieve<CustomerEntity>(partitionName, "Ben");

                // Execute the operation.
                TableResult retrievedResult3 = table.Execute(retrieveOperation3);

                // Assign the result to a CustomerEntity object.
                CustomerEntity updateEntity3 = (CustomerEntity)retrievedResult3.Result; // note the cast

                if (updateEntity3 != null)
                {
                    // Change the phone number.
                    updateEntity3.PhoneNumber = "425-555-0105";

                    // Create the Replace TableOperation.
                    TableOperation updateOperation3 = TableOperation.Replace(updateEntity3);

                    // Execute the operation.
                    table.Execute(updateOperation3);

                    Console.WriteLine("Ben's phone number updated.");
                }
                else
                {
                    Console.WriteLine("Entity could not be retrieved.");
                }
                #endregion


                #region DELETE
                // Create a retrieve operation that expects a customer entity.
                TableOperation retrieveOperation4 = TableOperation.Retrieve<CustomerEntity>(partitionName, "Walter");

                // Execute the operation.
                TableResult retrievedResult4 = table.Execute(retrieveOperation4);

                // Assign the result to a CustomerEntity.
                CustomerEntity deleteEntity4 = (CustomerEntity)retrievedResult4.Result; // note the cast

                // Create the Delete TableOperation.
                if (deleteEntity4 != null)
                {
                    TableOperation deleteOperation4 = TableOperation.Delete(deleteEntity4);

                    // Execute the operation.
                    table.Execute(deleteOperation4);

                    Console.WriteLine("Walter's entity deleted.");
                }
                else
                {
                    Console.WriteLine("Could not retrieve the entity.");
                }
                Console.WriteLine();
                #endregion

                // regions
                #region READ
                // Construct the query operation for all customer entities in partition called "My_Peoples_Partition".
                TableQuery<CustomerEntity> query5 = new TableQuery<CustomerEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionName));
                List<CustomerEntity> entityList5 = new List<CustomerEntity>(table.ExecuteQuery(query5));

                // Print the fields for each customer.
                foreach (CustomerEntity entity5 in entityList5)
                {
                    Console.WriteLine("{0}; {1}; {2}; {3}", entity5.PartitionKey, entity5.RowKey, entity5.Email, entity5.PhoneNumber);
                }
                Console.WriteLine();


                // Create a retrieve operation that takes a customer entity.
                TableOperation retrieveOperation5 = TableOperation.Retrieve<CustomerEntity>(partitionName, "Ben");

                // Execute the retrieve operation.
                TableResult retrievedResult5 = table.Execute(retrieveOperation5);

                // Print the phone number of the result.
                if (retrievedResult5.Result != null)
                {
                    Console.WriteLine("Ben's phone number: " + ((CustomerEntity)retrievedResult5.Result).PhoneNumber); // note the cast
                }
                else
                {
                    Console.WriteLine("Ben's phone number could not be retrieved.");
                }
                #endregion

            }
            catch (StorageException ex)
            {
                Console.WriteLine("oops..." + ex.Message);
                // Exception handling here.
            }
        }

    }
}
