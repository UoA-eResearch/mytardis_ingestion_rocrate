from . import ProjectDBFactory

class MetadataFactory():

    def __init__(self):
        self.factory = ProjectDBFactory()

    def get_metadata_from_research_drive(self,
                                         research_drive):
        print("Finding project associated with drive {0}".format(research_drive))
        code = research_drive.split('-')[0]
        try:
            proj_id = self.factory.get_project_id_from_code(code)
        except FileNotFoundError:
            print("Unable to find project")
        except Exception as error:
            raise
        print('Found project ID# {0} associated with drive {1}'.format(proj_id,
                                                                       research_drive))
            
