from rocrate import rocrate_api
from rocrate.model.person import Person
from rocrate.rocrate import ROCrate
from ProjectDB import MetadataFactory
import json, datetime

factory = MetadataFactory()
research_drive ='rescer201900004-data-transfer-tests'
factory.get_min_metadata_from_research_drive(research_drive)

crate = ROCrate() 
print(factory.metadata['title'])
print(factory.metadata['created_date'])

crate.name = factory.metadata['title']
crate.description = factory.metadata['description']
crate.datePublished = factory.metadata['created_date']

# wf_path = "test/test-data/test_galaxy_wf.ga"
# files_list = ["test/test-data/test_file_galaxy.txt"]

# Create base package
# wf_crate = rocrate_api.make_workflow_rocrate(workflow_path=wf_path,wf_type="Galaxy",include_files=files_list)

## adding a File entity:
# sample_file = './test.txt'
# files_list = "files/"
# file_entity = crate.add_file(files_list)

# Adding a File entity with a reference to an external (absolute) URI
# remote_file = crate.add_file('https://github.com/ResearchObject/ro-crate-py/blob/master/test/test-data/test_galaxy_wf.ga', fetch_remote = False)

# adding a Dataset
sample_dir = '/Volumes/rescer201900004-data-transfer-tests/hackday/archive/rcar004/'
# dataset_entity = crate.add_directory(sample_dir)

# dataset_entity = crate.add_directory(sample_dir, 'files/')
# Add authors info
# crate.add(Person(crate, '#joe', {'name': 'Joe Bloggs'}))

# wf_crate example
authors = []

for person in factory.metadata['people']:
    crate.add(Person(crate, '#'+str(person['id']), {'name': person['full_name'], 'email': person['email'] }))
    print( person['full_name'] )
    print( person['id'] )
    print( person['email'] )

# crate.license = 'MIT'
# crate.isBasedOn = "https://climate.usegalaxy.eu/u/annefou/w/workflow-constructed-from-history-climate-101"
# crate.keywords = ['GTN', 'climate']
# crate.image = "climate_101_workflow.svg"
# crate.CreativeWorkStatus = "Stable"

# write crate to disk
out_path = "./out-crate"
crate.write_crate(out_path)
