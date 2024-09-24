# RO-Crate Generation - My Tardis
Scripts for packaging instrument data for [ingestion](https://github.com/UoA-eResearch/mytardis_ingestion) into [MyTardis](https://github.com/UoA-eResearch/mytardis) as [Research Object Crates](https://w3id.org/ro/crate).
Along with modules for generating RO-Crates internally within MyTardis (to be developed).

## Installation

As these scripts are under active development currently to install do the following:

```sh
#clone this repo

git clone https://github.com/UoA-eResearch/mytardis_ingestion_rocrate

cd my_tardis_ro_crate

#setup the poetry environment
poetry config virtualenvs.in-project true
poetry install
```

Once installed, run `poetry shell` to activate the environment.
Any command is then run using the `ro_crate_builder` CLI such as `ro_crate_builder --help`


## RO-Crate Generation for MyTardis Ingestions
the defined input formats for RO-Crate generation are:


- Print Lab sample files
- ~~ABI Music Microscope data~~ (Updates required for ABI crate builder to reflect changes in MyTardis UUID assignment)

to generate a Print Lab RO-Crate run

```bash
ro_crate_builder print-lab -i /print_lab_dir/sampledata.xls -o /output_crate_location
```

where sampledata is a sheet of data regarding your samples with labels matching a MyTardis schema.
an example can be found [here](tests/examples_for_test/print_lab_test/sampledata.xlsx).

The `ro_crate_builder print-lab` command has the following optional parameters:


- `--bag_crate true|false` package the crate as a bagit (default true)
- `--split_datasets` create an individual RO-Crate for each dataset in the input data
- `-a [tar.gz|tar|zip]` archive the final crate as a specific format
- `--bulk_encrypt` bulk encrypt the entire output file to supplied pubkey fingerprints and MyTardis User (only available with the `-a` `tar` and `tar.gz` options)
- `--pubkey_fingerprints [gpg pubkey fingerprint]` encrypt any output file to these fingerprints when using `--bulk_encrypt`
- `--dry-run` generate metadata without moving or archiving any of the data itself
- `--tmp_dir [path/to/dir]` use a different temporary directory than the system default when archiving and encrypting
- `--separate_manifests` create a directory with a copy of the ro_crate_metadata.json and the bagit manifests outside of any archive

use `--help` for a full list of options.

### Ingestion .env configs
The RO-Crate builder scripts will use an `.env` config file used for [ingestion](https://github.com/UoA-eResearch/mytardis_ingestion) for API authentication and default schema config. It will look for those by default in the current directory; an alternative directory can be provided with the `--env_prefix` parameter.

An example .env file can be found [here](<example_.env >).

the following values can be overwritten via CLI inputs:
- `--mt_hostname` hostname for MyTardis API
- `--mt_api_key` API key for MyTardis API
- `--mt_user` username for MyTardis API

the following values may only be provided via the env file, and describe the MyTardis schemas that define metadata for their associated objects:
```shell
DEFAULT_SCHEMA__PROJECT=[MyTardis schema namespace]

DEFAULT_SCHEMA__EXPERIMENT=[MyTardis schema namespace]

DEFAULT_SCHEMA__DATASET=[MyTardis schema namespace]

DEFAULT_SCHEMA__DATAFILE=[MyTardis schema namespace]

mytardis_pubkey__key=[gpg public key fingerprint]

mytardis_pubkey__name=[string]
```
the `mytardis_pubkey__key` and `mytardis_pubkey__name` values will be used to encrypt any metadata for the MyTardis server ingesting the data.

The following values may be specified via a .env file which allows for API updating of ICD-11 values.

```shell
ICD11client_id=[ICD-11 id]

ICD11CLIENT_SECRET=[ICD-11 secret]
```

### Metadata collection
All ingestion target RO-Crates check appropriate MyTardis Schemas via the MyTardis API for metadata objects, creating MTmetadata objects if appropriate.


 The argument
`--collect-all` will load any data in the input format as both MTmetadata and RO-Create `'additional-properties' '` even if it does not match the schema.
This is an extreme "I must have everything backed up and potentially ingestible" option and is likely to invalidate the RO-Crate spec (although not in a way that will remove other valid data). This data will not be encrypted even if it is otherwise flagged as sensitive!


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
   "myTardis-type": {
   	"type": "string",
   	"description" : "Metadata type as recorded in MyTardis",
   	"default": "STRING"
	},
	"mytardis-schema":{
    	"type":"string",
    	"description":"The schema of this metadata in MyTardis"
	}
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
	"required": [ "@id", "@type", "name","value","myTardis-type","mytardis-schema ],
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
	"mytardis-schema": "http://print.lab.mockup/project/v1"
	"parents":[
    	"#BAM",
    	"#BAM_sorted"
    	]
}
```
## Encryption
MyTardis RO-Crates employ encryption of RO-Crate metadata provided by [UoA's RO-Crate Fork](https://github.com/UoA-eResearch/ro-crate-py/tree/encrypted-metadata).

All metadata marked as sensitive in MyTardis will be read into a PGP encrypted block encrypted against the keys provided to the script.

If no keys or binary are provided then sensitive metadata will not be read into the RO-Crate (usually resulting in an error if a key is expected but not found).

## Requirements
Mandatory:
- [poetry](https://python-poetry.org/docs/)
- [python3](https://www.python.org/downloads/) (version >=3.10,<=3.12.5)

Optional:
- a valid [GnuPG](https://www.gnupg.org/download/) binary (required for encryption)
- MyTardis API Key (required for user lookup in MyTardis)
- ICD11 API ID and secret (required for ICD-11 code lookup)


### The Art of Remembering
Forked from project:
* RO-Crate-ABI-Music
* (CeR Hackday project 2021: The Art of Remembering)
