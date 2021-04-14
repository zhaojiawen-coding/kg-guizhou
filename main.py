
import os
from tools.neo4j_initial import Neo4jInitial
from tools.data_process import process_scholar,process_paper,process_project,process_patent,process_scholar_publish_ship

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

URL = "***"
USERNAME = "***"
PASSWORD = "***"

if __name__ == '__main__':

    neo4j_link = Neo4jInitial(url=URL, username=USERNAME, password=PASSWORD)

    # test
    # scholar_file_path = "processed_data/scholar.json"
    # paper_file_path = "processed_data/paper.json"
    # project_file_path = "processed_data/project.json"
    # patent_file_path = "processed_data/patent.json"
    # scholar_publish_path = "processed_data/scholar_publish.json"

    scholar_file_path = "data/dump_scholar.json"
    paper_file_path = "data/dump_paper.json"
    project_file_path = "data/dump_project.json"
    patent_file_path = "data/dump_patent.json"
    scholar_publish_path = "data/dump_scholar_publish.json"

    scholar_source_file = os.path.join(BASE_DIR,scholar_file_path)
    paper_source_file = os.path.join(BASE_DIR,paper_file_path)
    project_source_file = os.path.join(BASE_DIR,project_file_path)
    patent_source_file = os.path.join(BASE_DIR,patent_file_path)
    scholar_publish_source_file = os.path.join(BASE_DIR,scholar_publish_path)

    process_scholar(scholar_source_file,neo4j_link)
    process_paper(paper_source_file,neo4j_link)
    process_project(project_source_file,neo4j_link)
    process_patent(patent_source_file,neo4j_link)
    process_scholar_publish_ship(scholar_publish_source_file,neo4j_link)

