import os
import sqlglot
import json
from get_seedQuery import SeedQueryGenerator
from data_structures.db_dialect import get_current_dialect


def _get_sqlglot_dialect_name() -> str:
    dialect = get_current_dialect()
    if dialect and dialect.name.upper() == "POSTGRESQL":
        return "postgres"
    return "mysql"

class Change:
    def __init__(self,file_path="./generated_sql/seedQuery.sql"):
        self.file_path=file_path
        self.seedqueries=self.get_queries()

    def get_queries(self):
        
        """逐行读取SQL查询文件并返回查询列表，跳过第一行的USE test;语句
        
        参数:
        - self.file_path: SQL查询文件的路径
        
        返回:
        - list: 包含所有SQL查询的列表
        """
        queries = []
        try:
            # 获取绝对路径
            abs_path = os.path.abspath(self.file_path)

            with open(abs_path, 'r', encoding='utf-8') as f:
                # 逐行读取文件
                for line_num, line in enumerate(f, 1):  # 从1开始计数行号
                    # 去除行首尾的空白字符
                    sql = line.strip()
                    # 忽略空行
                    if sql:
                        queries.append(sql)
            return queries
        except Exception as e:
            print(f"读取SQL文件时出错: {e}")
            return []

    def getAST(self,query):
        try:
            ast = sqlglot.parse_one(query, read=_get_sqlglot_dialect_name())
            print(ast)
            #print("=== 使用walk()遍历所有节点 ===")
            return ast
        except Exception as e:
            print(f"解析查询失败: {e}")
            return None
    def ASTChange(self,query):
        ast=self.getAST(query)
        return ast
       


