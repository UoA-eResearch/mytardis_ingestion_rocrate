from . import ProjectDBFactory
import time

class MetadataFactory():

    def __init__(self):
        self.factory = ProjectDBFactory()
        self.metadata = {}

    def get_min_metadata_from_research_drive(self,
                                         research_drive):
        print()
        print("Locating resources for drive {0}".format(research_drive))
        time.sleep(2)
        code = research_drive.split('-')[0]
        try:
            proj_id = self.factory.get_project_id_from_code(code)
        except FileNotFoundError:
            print("Unable to find project")
        except Exception as error:
            raise
        print('Found project ID# {0} associated with drive'.format(proj_id,
                                                                       research_drive))
        time.sleep(2)
        self.metadata['proj_id'] = proj_id
        print("Getting project info")
        print()
        try:
            title, description, created_date = self.factory.get_name_and_description_by_project_id(proj_id)
        except Exception as error:
            raise
        print("Project Name: {0}\nProject Creation Date: {2}".format(title,
                                                                        description,
                                                                        created_date))
        self.metadata['title'] = title
        self.metadata['description'] = description
        self.metadata['created_date'] = created_date
        # print("Getting people associated with project")
        try:
            project_owner = self.factory.get_people_ids_from_project(proj_id)
        except Exception as error:
            raise
        people = []
        for person in project_owner:
            p = self.factory.get_person_from_id(person)
            print("Person: {0} ({1})".format(p['full_name'], p['email']))
            people.append(p)
        self.metadata['people'] = people
        print()

