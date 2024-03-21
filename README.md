# RO-Crate Generation - My Tardis
Scripts for packaging instrument data for [ingestion](https://github.com/UoA-eResearch/mytardis_ingestion) into [MyTardis](https://github.com/UoA-eResearch/mytardis) as [Research Object Crates](https://w3id.org/ro/crate).
Along with modules for generating RO-Crates internally within MyTardis.






## Ingestion target RO-Crate Generation
All ingestion target RO-Crates check appropriate MyTardis Schemas for metadata objects, creating MTmetadata objects if appropriate. The argument `--collect-all` will load any data in the input format as both MTmetadata and RO-Create `'additional-properties' '` even if it does not match the schema.
This is an extreme "I must have everything backed up and potentially ingestible" option and is likely to invalidate the RO-Crate spec (although not in a way that will remove other valid data).


## RO-Crate objects unique to MyTardis
There are several entities in MyTardis RO-Crates that do not appear in schema.org specifically for ingestion and recovery of MyTardis metadata.


### MTmetadata object
The main entity that is unique to MyTardis RO-Crates is the MTmetadata object which contains all information specific to a piece of MyTardis Metadata used to recover this information and the associated schema if the MyTardis data or instance is lost.


```json
{
   "@id": {
       "type": "string",
       "description" : "unique ID in the RO-Crate"
    },
   "@type": "MyTardis-Metadata_field",
   "name": {
       "type": "string",
       "description" : "name of the metadata in MyTardis"
   },
   "value": {
       "description" : "Metadata value in my tardis"
    },
   "mt-type": {
       "type": "string",
       "description" : "Metadata type as recorded in MyTardis",
       "default": "STRING"
    },
   "sensitive": {
       "type": "bool",
       "description" : "Is this metadata marked as sensitive in MyTardis",
       "default": True
    },
    "required": [ "@id", "@type", "name","value","mt-type" ],
}
```
for example:
```json
{
    "@id": "#BAM_Analysis code",
    "@type": "my_tardis_metadata",
    "myTardis-type": "STRING",
    "name": "Analysis code",
    "sensitive": false,
    "value": "vs2"
}
```



### The Art of Remembering
Forked from project:
* RO-Crate-ABI-Music
* (CeR Hackday project 2021: The Art of Remembering)











