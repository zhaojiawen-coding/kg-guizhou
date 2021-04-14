# -*- coding: utf-8 -*-
from py2neo import Node,Graph,Relationship,NodeMatcher
import csv



class Neo4jInitial(object):
    """连接并建立Neo4j"""

    def __init__(self,url,username,password):
        """
        建立连接
        :param url:
        :param username:
        :param password:
        """
        link = Graph(url,username=username,password=password)

        self.graph = link
        #清除数据库，谨慎使用
        #### self.graph.delete_all()
        self.matcher = NodeMatcher(link)


    def create_node_from_dict(self,node_name_label_dict):
        """
        建立节点,从内存中读取
        格式：{name:label}
        :param node_list:
        :return:
        """
        for name,label in node_name_label_dict:
            node = Node(label,name=name)
            self.graph.create(node)

    def create_node_with_name_attributes(self,label,node_name,attr_dict):
        """
        单门把name属性拎出来了，防止忘了写name属性
        :param label:
        :param node_name:
        :param attr_dict:
        :return:
        """
        node = Node(label,name=node_name,**attr_dict)
        self.graph.create(node)

    def create_node_without_attr(self,label,name):
        # node = Node(label=label,name=name)
        # self.graph.create(node)
        cypher = self.merge_node_hypher_str(label, name)
        self.graph.run(cypher)

    def merge_node_hypher_str(self,label,name):
        return "MERGE (n:" + label + " {name:" + "\"" + name + "\"" + "})"

    def merge_relation_hypher_str(self,head_id,head_label,relation,tail_id,tail_label):
        return "MATCH (h:" + head_label +"),(t:" + tail_label + ") " \
                + "WHERE h.pid=" + str(head_id) + " and t.uid=" + str(tail_id) \
                + " MERGE (h)-[:" + relation + "]->(t)"

    def create_node_from_file(self,node_name_label_file):
        """
        建立节点,从文件中读取
        需要将文件组织为以下格式：
        name\tlabel
        例如：
        张三  人名
        :param node_label_name_file:
        :return:
        """
        with open(node_name_label_file,"r",encoding='utf-8') as f:
            csv_reader = csv.reader(f,delimiter="\t",quotechar="\"")
            for line in csv_reader:
                node = Node(line[1],name=line[0])
                self.graph.create(node)


    def create_relation_from_file(self,relation_file):
        """
        建立关系，从文件中获得
        文件中数据格式：head\trelation\ttail
        例如：语文   属于  学科
        :param relation_file:
        :return:
        """
        with open(relation_file,"r",encoding="utf-8") as f:
            csv_reader = csv.reader(f,delimiter="\t",quotechar="\"")
            for line in csv_reader:
                try:
                    head = self.matcher.match(name=line[0]).first()
                    tail = self.matcher.match(name=line[2]).first()
                    rel = Relationship(head,line[1],tail)
                    self.graph.create(rel)
                except AttributeError as e:
                    print(e)

    def create_relation(self,head,relation,tail):

        rel = Relationship(head,relation,tail)
        self.graph.create(rel)