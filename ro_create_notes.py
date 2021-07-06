from rocrate import rocrate_api

wf_path = "test/test-data/test_galaxy_wf.ga"
files_list = ["test/test-data/test_file_galaxy.txt"]

# Create base package
wf_crate = rocrate_api.make_workflow_rocrate(workflow_path=wf_path,wf_type="Galaxy",include_files=files_list)
## adding a File entity:
sample_file = '/path/to/sample_file.txt'
file_entity = crate.add_file(sample_file)

# Adding a File entity with a reference to an external (absolute) URI
remote_file = crate.add_file('https://github.com/ResearchObject/ro-crate-py/blob/master/test/test-data/test_galaxy_wf.ga', fetch_remote = False)

# adding a Dataset
sample_dir = '/path/to/dir'
dataset_entity = crate.add_directory(sample_dir, 'relative/rocrate/path')

from rocrate.model.person import Person

# Add authors info
crate.add(Person(crate, '#joe', {'name': 'Joe Bloggs'}))

# wf_crate example
publisher = Person(crate, '001', {'name': 'Bert Verlinden'})
creator = Person(crate, '002', {'name': 'Lee Ritenour'})
wf_crate.add(publisher, creator)

# These contextual entities can be assigned to other metadata properties:

wf_crate.publisher = publisher
wf_crate.creator = [ creator, publisher ]

wf_crate.license = 'MIT'
wf_crate.isBasedOn = "https://climate.usegalaxy.eu/u/annefou/w/workflow-constructed-from-history-climate-101"
wf_crate.name = 'Climate 101'
wf_crate.keywords = ['GTN', 'climate']
wf_crate.image = "climate_101_workflow.svg"
wf_crate.description = "The tutorial for this workflow can be found on Galaxy Training Network"
wf_crate.CreativeWorkStatus = "Stable"

# Write to zip file
out_path = "/home/test_user/crate"
crate.write_zip(out_path)

# write crate to disk
out_path = "/home/test_user/crate_base"
crate.write_crate(out_path)
