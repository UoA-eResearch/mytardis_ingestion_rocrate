from ProjectDB import MetadataFactory

factory = MetadataFactory()
research_drive ='rescer201900004-data-transfer-tests'

factory.get_min_metadata_from_research_drive(research_drive)
print(factory.metadata)
