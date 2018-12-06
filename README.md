# cosmosdb-changefeed-incremental-changes
Cosmos DB Change Feed - Capturing incremental changes along with the previous snapshot for each change

This sample highlights capturing incremental changes made to a document in Azure Cosmos DB.

The sample application does the following:

1. Creates a new collection
2. Replaces the DocumentCollection's ChangeFeedPolicy
3. Creates and writes a new document and updates the same document repeatedly
3.1. Finally, the document is deleted to highlight the differences between changes captured by ChangeFeed as well as a delete captured by ChangeFeed.
4. Executes ChangeFeed to fetch the incremental changes made to the document
   This is a new feature, currently in private preview, where previously, only the final snapshot of a changed document was captured.
   
The private preview version captures the following information:
1. The state of the document after the update.
2. The previous snapshot of the document prior to the update.
3. Metadata containing the details of the update.

In the case of a delete operation, the current and previous versions of the document are not returned by ChangeFeed. Only the metadata is returned by ChangeFeed.

Example of a change captured by ChangeFeed:
{
	"firstName": "Abinav",
	"lastName": "Rameesh",
	"employer": "Microsoft Corporation",
	"title": "Software Engineer",
	"age": 45,
	"undergraduateUniversity": "University of Michigan, Ann Arbor",
	"postGraduateUniversity": "Cornell University",
	"id": "Abinav",
	"_rid": "SCROAImok74BAAAAAAAAAA==",
	"_self": "dbs/SCROAA==/colls/SCROAImok74=/docs/SCROAImok74BAAAAAAAAAA==/",
	"_etag": "\"0000cc13-0000-0000-0000-5c0876a70000\"",
	"_attachments": "attachments/",
	"_ts": 1544058535,
	"_lsn": 18,
	"_metadata": {
		"operationType": "replace",
		"previousImage": {
			"firstName": "Abinav",
			"lastName": "Rameesh",
			"employer": "Microsoft Corporation",
			"title": "Software Engineer",
			"age": 44,
			"undergraduateUniversity": "University of Michigan, Ann Arbor",
			"postGraduateUniversity": "Cornell University",
			"id": "Abinav",
			"_rid": "SCROAImok74BAAAAAAAAAA==",
			"_self": "dbs/SCROAA==/colls/SCROAImok74=/docs/SCROAImok74BAAAAAAAAAA==/",
			"_etag": "\"0000cb13-0000-0000-0000-5c0876a70000\"",
			"_attachments": "attachments/",
			"_ts": 1544058535
		}
	}
}

Example of a delete captured by ChangeFeed:
{
	"id": "Abinav",
	"_lsn": 19,
	"_metadata": {
		"operationType": "delete",
		"previousImage": {
			"firstName": "Abinav",
			"lastName": "Rameesh",
			"employer": "Microsoft Corporation",
			"title": "Software Engineer",
			"age": 45,
			"undergraduateUniversity": "University of Michigan, Ann Arbor",
			"postGraduateUniversity": "Cornell University",
			"id": "Abinav",
			"_rid": "SCROANXOsmQBAAAAAAAAAA==",
			"_self": "dbs/SCROAA==/colls/SCROANXOsmQ=/docs/SCROANXOsmQBAAAAAAAAAA==/",
			"_etag": "\"29007809-0000-0000-0000-5c087f770000\"",
			"_attachments": "attachments/",
			"_ts": 1544060791
		}
	}
}

