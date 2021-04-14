import json
import re
from tqdm import tqdm,trange
import os

LABELS = {
    "PEOPLE": "people",
    "ORG": "org",
    "MAJOR_DIRECTION": "major_direction",
    "PAPER": "paper",
    "PROJECT": "project",
    "FIRST_LEVEL_DISCIPLINE": "first_level_discipline",
    "SECONDARY_DISCIPLINE": "secondary_discipline",
    "TERTIARY_DISCIPLINE": "tertiary_discipline",
    "PATENT": "patent",
}
Relations = {
    "PEOPLE_AND_MAJOR_DIRECTION": "major",
    "PEOPLE_AND_ORG": "belong_to_org",
    "PROJECT_AND_ORG": "pro_belong_to_org",
    "PROJECT_AND_FIRST_LEVEL_DISCIPLINE": "belong_to_first_level_discipline",
    "PROJECT_AND_SECONDARY_DISCIPLINE": "belong_to_secondary_discipline",
    "PROJECT_AND_TERTIARY_DISCIPLINE": "belong_to_tertiary_discipline",
    "PATENT_AND_ORG": "pat_belong_to_org",
    "PAPER_AND_SCHOLAR": "author",
    "PROJECT_AND_SCHOLAR": "leader",
    "PATENT_AND_SCHOLAR": "inventor",
}


##处理dump_scholar.json
def process_scholar(file_name, neo4j_link):
    """
    处理dump_scholar.json
    :param file_name:
    :return:
    """
    with open(file_name, "r", encoding="utf-8") as f:

        load_dict = json.load(f)

        print(" **** Starting creating nodes( %s %s %s )and relations( %s %s )**** " % (
        LABELS["PEOPLE"], LABELS["MAJOR_DIRECTION"], LABELS["ORG"],
        Relations["PEOPLE_AND_MAJOR_DIRECTION"], Relations["PEOPLE_AND_ORG"]))
        for dic in tqdm(load_dict):
            peo_attrs = {}
            if dic["name"] != None:

                for key ,val in dic.items():
                    if key == "id":
                        peo_attrs["uid"] = dic["id"]
                        continue
                    if key not in ["name","org_name","major"]:
                        peo_attrs[key] = dic[key]
                neo4j_link.create_node_with_name_attributes(LABELS["PEOPLE"], dic["name"], peo_attrs)

            if dic["major"] != None:
                majors = dic["major"]
                majors = re.split(',|，|、', majors.strip())
                for major in majors:

                    cypher_node = neo4j_link.merge_node_hypher_str(LABELS["MAJOR_DIRECTION"],dic["major"])
                    try:
                        neo4j_link.graph.run(cypher_node)
                    except Exception as e:
                        with open(os.path.join(os.path.dirname(os.path.abspath(file_name)), "checkpoint.txt"), "w",
                                  encoding="utf-8") as f:
                            f.write(cypher_node+"\n")
                            f.close()
                        print(e, cypher_node)
                    #需要去重
                    # neo4j_link.create_node_without_attr(LABELS["MAJOR_DIRECTION"], major)
                    if dic["name"] != None:
                        ##创建人和主修方向的连接
                        # 这种创建也可以，但是明显查找过程效率很低，所以改为使用原生语句
                        # head = neo4j_link.matcher.match(LABELS["PEOPLE"]).where(name= dic["name"],id= dic["id"]).first()
                        # tail = neo4j_link.matcher.match(LABELS["MAJOR_DIRECTION"]).where(name= major).first()
                        # neo4j_link.create_relation(head,Relations["PEOPLE_AND_MAJOR_DIRECTION"],tail)
                        cypher = "MATCH (p:" + LABELS["PEOPLE"] + "),(m:" + LABELS["MAJOR_DIRECTION"] + ")" \
                                 + " WHERE p.name =" + "\"" + str(dic["name"]) + "\"" + " and p.uid = " + str(
                            dic["id"]) + " and m.name = " + "\"" + major + "\"" \
                                 + " CREATE (p)-[:" + Relations["PEOPLE_AND_MAJOR_DIRECTION"] + "]->(m)"
                        try:
                            neo4j_link.graph.run(cypher)
                        except Exception as e:
                            with open(os.path.join(os.path.dirname(os.path.abspath(file_name)), "checkpoint.txt"), "w",
                                      encoding="utf-8") as f:
                                f.write(cypher+"\n")
                                f.close()
                            print(e, cypher)

            if dic["org_name"] != None:
                cypher_node = neo4j_link.merge_node_hypher_str(LABELS["ORG"],dic["org_name"])
                try:
                    neo4j_link.graph.run(cypher_node)
                except Exception as e:
                    with open(os.path.join(os.path.dirname(os.path.abspath(file_name)), "checkpoint.txt"), "w",
                              encoding="utf-8") as f:
                        f.write(cypher_node+"\n")
                        f.close()
                    print(e, cypher_node)
                # neo4j_link.create_node_without_attr(LABELS["ORG"], dic["org_name"])
                ##创建人和机构的连接
                if dic["name"] != None:
                    # head = neo4j_link.matcher.match(LABELS["PEOPLE"]).where(name=dic["name"], id=dic["id"]).first()
                    # tail = neo4j_link.matcher.match(LABELS["ORG"]).where(name=dic["org_name"]).first()
                    # neo4j_link.create_relation(head, Relations["PEOPLE_AND_ORG"], tail)
                    cypher = "MATCH (p:" + LABELS["PEOPLE"] + "),(o:" + LABELS["ORG"] + ") " \
                             + "WHERE p.name=" + "\"" + dic["name"] + "\" " + "and p.uid=" + str(
                        dic["id"]) + " and o.name=" + "\"" + dic["org_name"] + "\" " \
                             + "CREATE (p)-[:" + Relations["PEOPLE_AND_ORG"] + "]->(o)"
                    try:
                        neo4j_link.graph.run(cypher)
                    except Exception as e:
                        with open(os.path.join(os.path.dirname(os.path.abspath(file_name)), "checkpoint.txt"), "w",
                                  encoding="utf-8") as f:
                            f.write(cypher+"\n")
                            f.close()
                        print(e, cypher)

        print(" **** Creating nodes( %s %s %s )and relations( %s %s )finished **** " % (
        LABELS["PEOPLE"], LABELS["MAJOR_DIRECTION"], LABELS["ORG"],
        Relations["PEOPLE_AND_MAJOR_DIRECTION"], Relations["PEOPLE_AND_ORG"]))
        f.close()


# 处理dump_paper.json文件
def process_paper(file, neo4j_link):
    """

    :param file:
    :return:
    """
    with open(file, "r", encoding="utf-8") as f:
        load_dict = json.load(f)

        print(" **** Starting creating nodes( %s )**** " % (LABELS["PAPER"]))
        for dic in tqdm(load_dict):
            paper_attr = {}
            if dic["paper_title"] != None:
                for key,val in dic.items():
                    if key == "id":
                        paper_attr["pid"] = dic["id"]
                        continue
                    if key not in ["paper_title"]:
                        paper_attr[key] = dic[key]

                neo4j_link.create_node_with_name_attributes(LABELS["PAPER"], dic["paper_title"], paper_attr)
        print(" **** Creating nodes( %s )finished **** " % (LABELS["PAPER"]))
        f.close()


def process_project(project_file, neo4j_link):
    """
    处理项目文件 dump_project.json
    :param project_file:
    :param neo4j_link:
    :return:
    """

    with open(project_file, "r", encoding="utf-8") as f:

        pro_dict = json.load(f)

        print(" **** Starting creating nodes( %s %s %s %s )and relations( %s %s %s %s )**** " % (
            LABELS["PROJECT"], LABELS["FIRST_LEVEL_DISCIPLINE"], LABELS["SECONDARY_DISCIPLINE"],
            LABELS["TERTIARY_DISCIPLINE"],
            Relations["PROJECT_AND_ORG"], Relations["PROJECT_AND_FIRST_LEVEL_DISCIPLINE"],
            Relations["PROJECT_AND_SECONDARY_DISCIPLINE"], Relations["PROJECT_AND_TERTIARY_DISCIPLINE"]))

        for dic in tqdm(pro_dict):
            pro_attrs = {}
            if dic["project_title"] != None:
                for key,val in dic.items():
                    if key == "id":
                        pro_attrs["pid"] = dic["id"]
                        continue
                    if key not in ["org","discipline_first","discipline_secondary","discipline_tertiary","project_title"]:
                        pro_attrs[key] = dic[key]

                neo4j_link.create_node_with_name_attributes(LABELS["PROJECT"], dic["project_title"], pro_attrs)

            if dic["org"] != None:
                cypher_node = neo4j_link.merge_node_hypher_str(LABELS["ORG"],
                                                               dic["org"])
                try:
                    neo4j_link.graph.run(cypher_node)
                except Exception as e:
                    with open(os.path.join(os.path.dirname(os.path.abspath(project_file)), "checkpoint.txt"), "w",
                              encoding="utf-8") as f:
                        f.write(cypher_node+"\n")
                        f.close()
                    print(e, cypher_node)
                ##创建连接
                if dic["project_title"] != None:
                    dic["project_title"] = re.sub("”|\"","\'",dic["project_title"])
                    cypher = "MATCH (p:" + LABELS["PROJECT"] + "),(o:" + LABELS["ORG"] + ") " \
                             + "WHERE p.name=" + "\"" + dic["project_title"] + "\" " + "and p.pid=" + str(
                        dic["id"]) + " and o.name=" + "\"" + dic["org"] + "\" " \
                             + "CREATE (p)-[:" + Relations["PROJECT_AND_ORG"] + "]->(o)"
                    try:
                        neo4j_link.graph.run(cypher)
                    except Exception as e:
                        with open(os.path.join(os.path.dirname(os.path.abspath(project_file)), "checkpoint.txt"), "w",
                                  encoding="utf-8") as f:
                            f.write(cypher+"\n")
                            f.close()
                        print(e, cypher)


            if dic["discipline_first"] != None:

                cypher_node = neo4j_link.merge_node_hypher_str(LABELS["FIRST_LEVEL_DISCIPLINE"],dic["discipline_first"])
                try:
                    neo4j_link.graph.run(cypher_node)
                except Exception as e:
                    with open(os.path.join(os.path.dirname(os.path.abspath(project_file)), "checkpoint.txt"), "w",
                              encoding="utf-8") as f:
                        f.write(cypher_node+"\n")
                        f.close()
                    print(e, cypher_node)
                #需要去重
                # neo4j_link.create_node_without_attr(LABELS["FIRST_LEVEL_DISCIPLINE"], dic["discipline_first"])
                ##创建连接
                if dic["project_title"] != None:
                    dic["project_title"] = re.sub("”|\"", "\'", dic["project_title"])
                    cypher = "MATCH (p:" + LABELS["PROJECT"] + "),(f:" + LABELS["FIRST_LEVEL_DISCIPLINE"] + ") " \
                             + "WHERE p.name=" + "\"" + dic["project_title"] + "\" " + "and p.pid=" + str(
                        dic["id"]) + " and f.name=" + "\"" + dic["discipline_first"] + "\" " \
                             + "CREATE (p)-[:" + Relations["PROJECT_AND_FIRST_LEVEL_DISCIPLINE"] + "]->(f)"
                    try:
                        neo4j_link.graph.run(cypher)
                    except Exception as e:
                        with open(os.path.join(os.path.dirname(os.path.abspath(project_file)), "checkpoint.txt"), "w",
                                  encoding="utf-8") as f:
                            f.write(cypher+"\n")
                            f.close()
                        print(e, cypher)

            if dic["discipline_secondary"] != None:

                cypher_node = neo4j_link.merge_node_hypher_str(LABELS["SECONDARY_DISCIPLINE"],dic["discipline_secondary"])
                try:
                    neo4j_link.graph.run(cypher_node)
                except Exception as e:
                    with open(os.path.join(os.path.dirname(os.path.abspath(project_file)), "checkpoint.txt"), "w",
                              encoding="utf-8") as f:
                        f.write(cypher_node+"\n")
                        f.close()
                    print(e, cypher_node)
                # neo4j_link.create_node_without_attr(LABELS["SECONDARY_DISCIPLINE"], dic["discipline_secondary"])
                ##创建连接
                if dic["project_title"] != None:
                    dic["project_title"] = re.sub("”|\"", "\'", dic["project_title"])
                    cypher = "MATCH (p:" + LABELS["PROJECT"] + "),(s:" + LABELS["SECONDARY_DISCIPLINE"] + ") " \
                             + "WHERE p.name=" + "\"" + dic["project_title"] + "\" " + "and p.pid=" + str(
                        dic["id"]) + " and s.name=" + "\"" + dic["discipline_secondary"] + "\" " \
                             + "CREATE (p)-[:" + Relations["PROJECT_AND_SECONDARY_DISCIPLINE"] + "]->(s)"
                    try:
                        neo4j_link.graph.run(cypher)
                    except Exception as e:
                        with open(os.path.join(os.path.dirname(os.path.abspath(project_file)), "checkpoint.txt"), "w",
                                  encoding="utf-8") as f:
                            f.write(cypher+"\n")
                            f.close()
                        print(e, cypher)

            if dic["discipline_tertiary"] != None:

                cypher_node = neo4j_link.merge_node_hypher_str(LABELS["TERTIARY_DISCIPLINE"],dic["discipline_tertiary"])
                try:
                    neo4j_link.graph.run(cypher_node)
                except Exception as e:
                    with open(os.path.join(os.path.dirname(os.path.abspath(project_file)), "checkpoint.txt"), "w",
                              encoding="utf-8") as f:
                        f.write(cypher_node+"\n")
                        f.close()
                    print(e, cypher_node)

                # neo4j_link.create_node_without_attr(LABELS["TERTIARY_DISCIPLINE"], dic["discipline_tertiary"])
                ##创建连接
                if dic["project_title"] != None:
                    dic["project_title"] = re.sub("”|\"", "\'", dic["project_title"])
                    cypher = "MATCH (p:" + LABELS["PROJECT"] + "),(t:" + LABELS["TERTIARY_DISCIPLINE"] + ") " \
                             + "WHERE p.name=" + "\"" + dic["project_title"] + "\" " + "and p.pid=" + str(
                        dic["id"]) + " and t.name=" + "\"" + dic["discipline_tertiary"] + "\" " \
                             + "CREATE (p)-[:" + Relations["PROJECT_AND_TERTIARY_DISCIPLINE"] + "]->(t)"
                    try:
                        neo4j_link.graph.run(cypher)
                    except Exception as e:
                        with open(os.path.join(os.path.dirname(os.path.abspath(project_file)), "checkpoint.txt"), "w",
                                  encoding="utf-8") as f:
                            f.write(cypher+"\n")
                            f.close()
                        print(e, cypher)
        print(" **** Creating nodes( %s %s %s %s )and relations( %s %s %s %s ) finished. **** " % (
            LABELS["PROJECT"], LABELS["FIRST_LEVEL_DISCIPLINE"], LABELS["SECONDARY_DISCIPLINE"],
            LABELS["TERTIARY_DISCIPLINE"],
            Relations["PROJECT_AND_ORG"], Relations["PROJECT_AND_FIRST_LEVEL_DISCIPLINE"],
            Relations["PROJECT_AND_SECONDARY_DISCIPLINE"], Relations["PROJECT_AND_TERTIARY_DISCIPLINE"]))
        f.close()


def process_patent(patent_file,neo4j_link):
    """
    处理专利 dump_patent.json
    :param patent_file:
    :param neo4j_link:
    :return:
    """
    with open(patent_file,"r",encoding="utf-8") as f:
        patent_dict = json.load(f)

        print(" **** Starting creating nodes:( %s ) ,relations:( %s ) **** " % (LABELS["PATENT"],Relations["PATENT_AND_ORG"]))

        for dic in tqdm(patent_dict):
            patent_attrs = {}
            if dic["patent_title"] != None:
                for key,val in dic.items():
                    if key == "id":
                        patent_attrs["pid"] = dic["id"]
                        continue
                    if key not in ["id","patent_title","applicant_name"]:
                        patent_attrs[key] = dic[key]

                neo4j_link.create_node_with_name_attributes(LABELS["PATENT"],dic["patent_title"],patent_attrs)

            if dic["applicant_name"] != None:
                orgs = dic["applicant_name"].split(";")
                for org in orgs:
                    cypher_org = neo4j_link.merge_node_hypher_str(LABELS["ORG"], org)
                    try:
                        neo4j_link.graph.run(cypher_org)
                    except Exception as e:
                        with open(os.path.join(os.path.dirname(os.path.abspath(patent_file)), "checkpoint.txt"), "w",
                                  encoding="utf-8") as f:
                            f.write(cypher_org+"\n")
                            f.close()
                        print(e, cypher_org)

                    if dic["patent_title"] != None:
                        cypher = "MATCH (p:" + LABELS["PATENT"] + "),(o:" + LABELS["ORG"] + ") " \
                                 + "WHERE p.name=" + "\"" + dic["patent_title"] + "\"" + " and p.id=" + str(
                            dic["id"]) + " and o.name=" + "\"" + org + "\" " \
                                 + "CREATE (p)-[:" + Relations["PATENT_AND_ORG"] + "]->(o)"
                        try:
                            neo4j_link.graph.run(cypher)
                        except Exception as e:
                            with open(os.path.join(os.path.dirname(os.path.abspath(patent_file)), "checkpoint.txt"), "w",
                                      encoding="utf-8") as f:
                                f.write(cypher+"\n")
                                f.close()
                            print(e, cypher)

        print(" **** Creating nodes:( %s ) ,relations:( %s ) finished. **** " % (LABELS["PATENT"],Relations["PATENT_AND_ORG"]))
        f.close()

def process_scholar_publish_ship(file,neo4j_link):
    """
    处理 dump_scholar_publish.json
    :param file:
    :param neo4j_link:
    :return:
    """
    with open(file,"r",encoding="utf-8") as f:

        realtions_dicts = json.load(f)

        print(" **** Begining creating relations:( %s %s %s) . **** " % (
            Relations["PAPER_AND_SCHOLAR"], Relations["PROJECT_AND_SCHOLAR"], Relations["PATENT_AND_SCHOLAR"]))
        for i,dic in tqdm(enumerate(realtions_dicts)):
            cypher = ""
            if dic["publish_type"] == "Paper":
                cypher = neo4j_link.merge_relation_hypher_str(dic["publish_id"],LABELS["PAPER"],Relations["PAPER_AND_SCHOLAR"],dic["scholar_id"],LABELS["PEOPLE"])
            elif dic["publish_type"] == "Project":
                cypher = neo4j_link.merge_relation_hypher_str(dic["publish_id"], LABELS["PROJECT"],
                                                              Relations["PROJECT_AND_SCHOLAR"], dic["scholar_id"],
                                                              LABELS["PEOPLE"])
            elif dic["publish_type"] == "Patent":
                cypher = neo4j_link.merge_relation_hypher_str(dic["publish_id"], LABELS["PATENT"],
                                                              Relations["PATENT_AND_SCHOLAR"], dic["scholar_id"],
                                                            LABELS["PEOPLE"])
            try:
                neo4j_link.graph.run(cypher)
            except Exception as e:
                with open(os.path.join(os.path.dirname(os.path.abspath(file)),"checkpoint.txt"),"w",encoding="utf-8") as f:
                    f.write(cypher+"\n")
                    f.close()
                print(e,cypher)
        print(" **** Creating relations:( %s %s %s) finished. **** " % (Relations["PAPER_AND_SCHOLAR"],Relations["PROJECT_AND_SCHOLAR"],Relations["PATENT_AND_SCHOLAR"]))
        f.close()



