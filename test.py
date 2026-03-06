
from get_seedQuery import SeedQueryGenerator
from generate_random_sql import Generate
from changeAST import PreSolve
from data_structures.table import Table, Column
from data_structures.db_dialect import set_dialect, DBDialect, get_current_dialect



def create_sample_tables():
    """创建示例表结构"""
    # 表t1
    t1 = Table(
        name="t1",
        columns=[
            Column("c1", "INT", "numeric", False, "t1"),
            Column("c2", "VARCHAR(255)", "string", False, "t1"),
            Column("c3", "VARCHAR(255)", "string", True, "t1"),
            Column("c4", "INT", "numeric", True, "t1"),
            Column("c5", "DATE", "datetime", False, "t1"),
            Column("c6", "VARCHAR(10)", "string", False, "t1")
        ],
        primary_key="c1",
        foreign_keys=[]
    )

    # 表t2 - 包含更多MySQL数据类型
    t2 = Table(
        name="t2",
        columns=[
            Column("c1", "INT", "numeric", False, "t2"),  # 主键
            Column("c2", "INT", "numeric", False, "t2"),  # 外键，关联t1.c1
            Column("c3", "DECIMAL(10,2)", "numeric", False, "t2"),  # 精确小数类型
            Column("c4", "VARCHAR(50)", "string", False, "t2"),  # 字符串类型
            Column("c5", "DATE", "datetime", False, "t2"),  # 日期类型
            Column("c6", "MEDIUMTEXT", "string", True, "t2"),  # 中等文本类型
            Column("c7", "LONGTEXT", "string", True, "t2"),  # 长文本类型
            Column("c8", "MEDIUMBLOB", "binary", True, "t2"),  # 中等二进制对象
            Column("c9", "LONGBLOB", "binary", True, "t2"),  # 长二进制对象
            Column("c10", "ENUM('value1','value2','value3')", "string", True, "t2"),  # 枚举类型
            Column("c11", "SET('a','b','c','d')", "string", True, "t2"),  # 集合类型
            Column("c12", "BIT(8)", "binary", True, "t2"),  # 位类型
            Column("c13", "DATETIME", "datetime", True, "t2"),  # 日期时间类型
            Column("c14", "FLOAT(8,2)", "numeric", True, "t2"),  # 带精度的浮点数
            Column("c15", "DOUBLE(12,4)", "numeric", True, "t2"),  # 带精度的双精度浮点数
            Column("c16", "JSON", "json", True, "t2")  # JSON数据类型，用于演示JSON支持
        ],
        primary_key="c1",
        foreign_keys=[{"column": "c2", "ref_table": "t1", "ref_column": "c1"}]
    )



    # 表t3 - 新增表，展示更多MySQL数据类型和关联关系
    t3 = Table(
        name="t3",
        columns=[
            Column("c1", "INT", "numeric", False, "t3"),  # 主键
            Column("c2", "INT", "numeric", False, "t3"),  # 外键，关联t1.c1
            Column("c3", "INT", "numeric", False, "t3"),  # 外键，关联t2.c1
            Column("c4", "YEAR", "datetime", False, "t3"),  # 年份类型
            Column("c5", "TIME", "datetime", True, "t3"),  # 时间类型
            Column("c6", "TINYINT", "numeric", True, "t3"),  # 微小整数类型
            Column("c7", "SMALLINT", "numeric", True, "t3"),  # 小整数类型
            Column("c8", "MEDIUMINT", "numeric", True, "t3"),  # 中整数类型
            Column("c9", "BIGINT", "numeric", True, "t3"),  # 大整数类型
            Column("c10", "LONGTEXT", "string", True, "t3"),  # 将JSON改为LONGTEXT以兼容MariaDB
            Column("c11", "VARCHAR(255)" if get_current_dialect().name == "TIDB" else "GEOMETRY", "binary", True, "t3"),  # 几何类型，TiDB方言使用VARCHAR(255)替代
            Column("c12", "TINYTEXT", "string", True, "t3"),  # 微小文本类型
            Column("c13", "TINYBLOB", "binary", True, "t3"),  # 微小二进制对象
            Column("c14", "SET('x','y','z')", "string", True, "t3"),  # 集合类型
            Column("c15", "TINYINT(1)", "numeric", True, "t3")  # 将BOOLEAN改为TINYINT(1)以兼容MariaDB
        ],
        primary_key="c1",
        foreign_keys=[
            {"column": "c2", "ref_table": "t1", "ref_column": "c1"},
            {"column": "c3", "ref_table": "t2", "ref_column": "c1"}
        ]
    )

    return [t1, t2, t3]

if __name__ == '__main__':
    # 设置数据库方言 - 可以选择"MYSQL"或"POSTGRESQL"
    # 当前设置为MySQL方言
    set_dialect("mysql")
    
    # 创建并设置全局表结构
    from generate_random_sql import set_tables
    tables = create_sample_tables()
    set_tables(tables)
    print("全局表结构已设置完成")
    
    #是否采用扩展
    use_extension=True
    # 调用Generate函数生成SQL

    presolve=PreSolve(extension=use_extension)
    #”math_equivalence“,"aggregate"
    presolve.presolve(
        aggregate_mutation_type="slot_m4",
    )
