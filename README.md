# RO-Crate Generation - My Tardis
Scripts for packaging instrument data for [ingestion](https://github.com/UoA-eResearch/mytardis_ingestion) into [MyTardis](https://github.com/UoA-eResearch/mytardis) as [Research Object Crates](https://w3id.org/ro/crate).
Along with modules for generating RO-Crates internally within MyTardis.



## Ingestion target RO-Crate Generation
the defined input formats for RO-Crate generation are:

- ABI Music Microscope data
- Print Lab sample files

to generate an RO-Crate for a collection of ABI Music datasets run:
```bash
ro_crate_builder abi -i /root_of_abi_directory
```
The root directory must contain a tree of directories with apropriate `project.json`, `experiment.json` and `dataset.json` files

to generate a Print Lab RO-Crate run

```bash
ro_crate_builder print-lab /print_lab_dir/sampledata.xls
```

where sampledata is a sheet of data regarding your samples with labels matching a mytardis schema

### Ingestion .env configs
The RO-Crate builder scripts will use a a `.env` config file used for [ingestion](https://github.com/UoA-eResearch/mytardis_ingestion) for API authentication and default schema config. It will look for those by default in the current directory, an alternative directory can be provided with the j `--env_prefix` parameter.

the following values can be overwritten via CLI inputs:
- `--mt_hostname` hostname for MyTardis API
- `--mt_api_key` API key for MyTardis API
- `--mt_user` username for MyTardis API

### Metadata collection
All ingestion target RO-Crates check appropriate MyTardis Schemas via the MyTardis API for metadata objects, creating MTmetadata objects if appropriate.


 The argument
`--collect-all` will load any data in the input format as both MTmetadata and RO-Create `'additional-properties' '` even if it does not match the schema.
This is an extreme "I must have everything backed up and potentially ingestible" option and is likely to invalidate the RO-Crate spec (although not in a way that will remove other valid data).


## RO-Crate objects unique to MyTardis
There are several entities in MyTardis RO-Crates that do not appear in schema.org specifically for ingestion and recovery of MyTardis metadata.


### MTmetadata object
The main entity that is unique to MyTardis RO-Crates is the MTmetadata object which contains all information specific to a piece of MyTardis Metadata. It can be used to recover this information and the associated schema if the MyTardis data or instance is lost.

There is usually one MTmetadata object per piece of unique metadata on a MyTardis Object in the crate.
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
       "description" : "Is this metadata marked as sensitive in MyTardis, used to encrypt metadata",
       "default": True
    },
    "Parents": {
       "type": "array",
       "items" : {
        "type": "string"
       },
       "description" : "The ID of any entity this metadata is associated with in the crate",
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
    "value": "vs2",
    "parents":[
        "#BAM",
        "#BAM_sorted"
        ]
}
```

### The Art of Remembering
Forked from project:
* RO-Crate-ABI-Music
* (CeR Hackday project 2021: The Art of Remembering)











