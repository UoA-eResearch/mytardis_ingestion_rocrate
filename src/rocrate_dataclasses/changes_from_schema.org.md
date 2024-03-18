# Changes from Schema.org in Print Lab RO-Crate dataclasses

Guiding principles of changes:

- Only add a field that deviates from schema.org or bioschemas if this field is ONLY relevant to MyTardis
- If possible provide information in a schema.org compatible way, even if this results in duplicate data in the Metadata for MT

## The Metadata object

A novel type has been defined to store arbitrary MyTardis Metadata.
This object is defined as follows:
```json
{
    "@id": {
        "type" : "string",
        "description": "the ID as it appears in the RO-Crate"
    },
    "@type": "MyTardis-Metadata_field",
    "name":{
        "type" : "string",
        "description": "the name of the metadata field in the MyTardis Schema"
    },
    "value": {
        "type" : "string",
        "description": "the value of this piece of metadata"
    },
    "mt-type": {
        "type" : "string",
        "description": "the type this metadata has in MyTardis"
    },
    "sensitive": {
        "type" : "bool",
        "description": "is this data marked as sensitive
        (and therefore behind Access Level Controls in MyTardis and Encrypted in the RO-Crate)"
    },
}
```
## Changes to Standard Dataclases
### Project
Projects remain largely unchanged from https://schema.org/Project.
* a `metadata` feild is added containing a list of all the ID's to relevant metadata objects for the project
* A `mytardis_classification` field that that must be one of the following strings `["RESTRICTED","SENSITIVE", "INTERNAL", "PUBLIC"]`

A project MUST contain the following to be accepted into MyTardis:

* an `@id` or `name` to act as the project name in MyTardis
* one `description` as a string
* one `principal_investigator` or `founder` that links to a valid Person object
* one `mytardis_classification` with a valid input

A project MAY contain any of the following


```json
    "project":{
        "@id": {
            "type" : "string",
            "description": "the ID as it appears in the RO-Crate and will be used to name the project in My Tardis"
        },
        "@type": "Project",
        "name":{
            "type" : "string",
            "description": "The name of the project in MyTardis"
        },
        "description": {
            "type" : "string",
            "description": "description of the item and project"
        },
        "mytardis_classification": {
            "type" : "string",
            "enum": ["RESTRICTED","SENSITIVE", "INTERNAL", "PUBLIC"]
        },
    }
,
```

### Experiment/Data Catalog

```
```

### Dataset

```
```

### Datafile

```
```