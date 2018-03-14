using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestTables
{
    public class CustomerEntity : TableEntity
    {
        public string Email { get; set; }
        public string PhoneNumber { get; set; }

        public CustomerEntity(string lastName, string firstName)
        {
            PartitionKey = lastName;
            RowKey = firstName;
        }

        public CustomerEntity() { }
    }
}
