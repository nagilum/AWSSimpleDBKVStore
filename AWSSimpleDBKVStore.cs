using System.Collections.Generic;
using System.Linq;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using Amazon.SimpleDB;
using Amazon.SimpleDB.Model;

namespace AWS {
    public class AWSSimpleDBKVStore {
        public string AccessKey { get; set; }

        public string SecretKey { get; set; }

        public RegionEndpoint RegionEndpoint { get; set; }

        public AWSSimpleDBKVStore(string accessKey, string secretKey, RegionEndpoint regionEndpoint) {
            this.AccessKey = accessKey;
            this.SecretKey = secretKey;
            this.RegionEndpoint = regionEndpoint;
        }

        public void Set(Dictionary<string, string> values, string domainName, string itemName) {
            var client = new AmazonSimpleDBClient(
                new BasicAWSCredentials(
                    this.AccessKey,
                    this.SecretKey),
                this.RegionEndpoint);

            var unused1 = client.CreateDomainAsync(
                new CreateDomainRequest(
                    domainName)).Result;

            var list = new AutoConstructedList<ReplaceableAttribute>();

            list.AddRange(
                values.Select(i => new ReplaceableAttribute(i.Key, i.Value, true)));

            var unused2 = client.PutAttributesAsync(
                new PutAttributesRequest(
                    domainName,
                    itemName,
                    list)).Result;
        }

        public Dictionary<string, string> Get(string domainName, string itemName) {
            var client = new AmazonSimpleDBClient(
                new BasicAWSCredentials(
                    this.AccessKey,
                    this.SecretKey),
                this.RegionEndpoint);

            var unused1 = client.CreateDomainAsync(
                new CreateDomainRequest(
                    domainName)).Result;

            var result = client.GetAttributesAsync(
                new GetAttributesRequest(
                    domainName,
                    itemName)).Result;

            return result.Attributes.ToDictionary(attr => attr.Name, attr => attr.Value);
        }
    }
}