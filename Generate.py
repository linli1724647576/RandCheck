from generate_random_sql import Generate
from get_seedQuery import SeedQueryGenerator
from changeAST import PreSolve
from data_structures.db_dialect import set_dialect, DBDialect
if __name__ == "__main__":
    set_dialect("mysql")
    
    use_extension=True
    # 使用示例表结构（默认行为）
    print("使用示例表结构生成SQL...")
    Generate(
        subquery_depth=1,  # 子查询深度为1
        total_insert_statements=50,  # 减少插入语句数量
        num_queries=100,  # 减少查询语句数量，便于观察
        query_type='default',
        use_database_tables=False  # 不使用数据库表结构
    )
        
    seed_query_generator=SeedQueryGenerator()
    seed_query_generator.get_seedQuery()
    #presolve=PreSolve(extension=use_extension)
    #”math_equivalence“,"aggregate"
    #presolve.presolve(
    #    aggregate_mutation_type="math_equivalence",
    #)
    # 以下是使用数据库表结构的示例配置

    # 注意：实际使用时需要根据您的数据库配置进行修改
    # db_config = {
    #     'host': '127.0.0.1',
    #     'port': 3306,
    #     'database': 'your_database_name',
    #     'user': 'your_username',
    #     'password': 'your_password',
    #     'dialect': 'MYSQL'  # 支持'MYSQL'和'POSTGRESQL'
    # }
    # 
    # print("使用数据库表结构生成SQL...")
    # Generate(
    #     subquery_depth=1,
    #     total_insert_statements=5,
    #     num_queries=1000,
    #     query_type='default',
    #     use_database_tables=True,  # 使用数据库表结构
    #     db_config=db_config
    # )