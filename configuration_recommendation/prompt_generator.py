tasks = [
    ("indexes_recommendation", "index_context"),
    ("materialised_views_recommendation", "matview_context"),
    ("knob_tuning", "knob_context"),
    ("optimization_plan_review", "review_context")
]

index_context = matview_context = knob_context = review_context = ""

for task_name, var_name in tasks:
    file_path = fr"../workload_compression/{task_name}_features.json"
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            if var_name == "index_context":
                index_context = content
            elif var_name == "matview_context":
                matview_context = content
            elif var_name == "knob_context":
                knob_context = content
            elif var_name == "review_context":
                review_context = content

    except FileNotFoundError:
        print(f"File not found for task: {task_name}")

OLAP_environment = """
    - Workload: OLAP, JOB(join-order-benchmark) contains 113 multi-joint queries with realistic and complex joins, Read-Only, execute sequentially .
    - Data: 13 GB data contains 50 tables and each table contains 1,000,000 rows of record.
    - Database Kernel: PostgreSQL v15.
    - Hardware: 8 vCPUs and 16 GB RAM, Disk Type: HDD.
"""
db_metric = "latency"

current_configuration = "Default"

join_condition = """
    movie_companies.company_type_id=company_type.id
    company_name.id=movie_companies.company_id
    keyword.id=movie_keyword.keyword_id
    movie_info_idx.movie_id=movie_companies.movie_id,movie_info.movie_id,cast_info.movie_id
    info_type.id=movie_info_idx.info_type_id
    movie_keyword.movie_id=title.id,movie_companies.movie_id,cast_info.movie_id
    cast_info.person_id=name.id,aka_name.person_id
"""
current_indexes = """ 
{
    person_id_aka_name ON aka_name (person_id),
    kind_id_aka_title ON aka_title (kind_id),
    movie_id_aka_title ON aka_title (movie_id),
    movie_id_cast_info ON cast_info (movie_id),
    person_id_cast_info ON cast_info (person_id),
    person_role_id_cast_info ON cast_info (person_role_id),
    role_id_cast_info ON cast_info (role_id),
    movie_id_complete_cast ON complete_cast (movie_id),
    company_id_movie_companies ON movie_companies (company_id),
    company_type_id_movie_companies ON movie_companies (company_type_id),
    movie_id_movie_companies ON movie_companies (movie_id),
    info_type_id_movie_info ON movie_info (info_type_id),
    movie_id_movie_info ON movie_info (movie_id),
    info_type_id_movie_info_idx ON movie_info_idx (info_type_id),
    movie_id_movie_info_idx ON movie_info_idx (movie_id),
    keyword_id_movie_keyword ON movie_keyword (keyword_id),
    movie_id_movie_keyword ON movie_keyword (movie_id),
    linked_movie_id_movie_link ON movie_link (linked_movie_id),
    link_type_id_movie_link ON movie_link (link_type_id),
    movie_id_movie_link ON movie_link (movie_id),
    info_type_id_person_info ON person_info (info_type_id),
    person_id_person_info ON person_info (person_id),
    kind_id_title ON title (kind_id)
}
"""


def _build_pg_conn_str() -> str:
    cfg = configparser.ConfigParser()
    # config.ini is one level up from this file
    cfg.read(os.path.join(os.path.dirname(__file__), '..', 'config.ini'), encoding='utf-8')
    section = 'configuration recommender'
    host = cfg.get(section, 'PG_Host', fallback='')
    port = cfg.get(section, 'PG_Port', fallback='5432')
    user = cfg.get(section, 'PG_User', fallback='postgres')
    password = cfg.get(section, 'PG_Password', fallback='')
    dbname = cfg.get(section, 'PG_DB', fallback='postgres')
    if not host:
        raise ValueError("Missing PG_Host in config.ini [configuration recommender]")
    return f"host={host} port={port} user={user} password={password} dbname={dbname}"


def refresh_context() -> Dict[str, Any]:
    """
    Re-extract features for all tasks and refresh in-memory contexts.
    Returns a dict with status info per task.
    """
    global index_context, matview_context, knob_context, review_context

    # Load extraction function dynamically to avoid package path issues
    gf_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'workload_compression', 'get_features.py'))
    spec = importlib.util.spec_from_file_location("get_features_module", gf_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load workload_compression/get_features.py")
    get_features_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(get_features_module)  # type: ignore[attr-defined]

    conn_str = _build_pg_conn_str()

    results: Dict[str, Any] = {}
    out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'workload_compression'))

    for task_name, var_name in tasks:
        try:
            feats = get_features_module.extract_features(conn_str, task_name)  # type: ignore[attr-defined]
            out_path = os.path.join(out_dir, f"{task_name}_features.json")
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(feats, f, ensure_ascii=False, indent=2)

            content = json.dumps(feats, ensure_ascii=False, indent=2)
            if var_name == "index_context":
                index_context = content
            elif var_name == "matview_context":
                matview_context = content
            elif var_name == "knob_context":
                knob_context = content
            elif var_name == "review_context":
                review_context = content
            results[task_name] = {"ok": True, "path": out_path}
        except Exception as e:
            results[task_name] = {"ok": False, "error": str(e)}

    return results

def get_question_analysis_prompt(question_domain):
    question_analyzer = f"You are an experienced database administrators, skilled in database {question_domain}. " 
    if question_domain == 'materialised views recommendation':
        prompt_get_question_analysis = """
            Task Overview: 
            Recommend optimal {question_domain} based on the inner metrics and workload characteristics in order to optimize the {db_metric} metric.
            Workload and database kernel information: 
            {environment}
            Workload Features: {content}
            Output Format:
            - Output must be a valid JSON object.
            - The object must contain:
            - "items": a list of materialized view recommendations. Each item must have:
                - "name": the materialized view name
                - "query": the SQL query that defines the materialized view
                - "details": (optional) explanation of why this materialized view helps
            - "rationale": a short overall explanation for the recommendations.

            Example:
            {{
            "items": [
                {{"name": "mv_top_customers", "query": "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id", "details": "Precomputes aggregation for top customers"}}
            ],
            "rationale": "Pre-aggregating common queries reduces repeated computation."
            }}
            Now, let's think step by step.
        """.format(question_domain = question_domain, db_metric = db_metric, environment=OLAP_environment, content=content)
    elif question_domain == 'indexes recommendation':
        prompt_get_question_analysis = """
            Task Overview: 
            Recommend optimal indexes based on the inner metrics and workload characteristics in order to optimize the {db_metric} metric.
            Workload and database kernel information: 
            {environment}
            Workload Features: {content}
            Output Format:
            - Output must be a valid JSON object.
            - The object must contain:
            - "items": a list of index recommendations. Each item must have:
                - "name": the index name
                - "table": the table where the index will be created
                - "columns": the columns to be indexed
                - "details": (optional) explanation of why this index helps
            - "rationale": a short overall explanation for the recommendations.

            Example:
            {{
            "items": [
                {{"name": "idx_orders_customer", "table": "orders", "columns": ["customer_id"], "details": "Speeds up lookups by customer"}}
            ],
            "rationale": "Adding selective indexes reduces scan costs for common queries."
            }}
        """.format(question_domain = question_domain, db_metric = db_metric, environment=OLAP_environment, content=content)
    elif question_domain =="knob tuning":
        prompt_get_question_analysis = """
            Task Overview: 
            Recommend optimal knob configuration based on the inner metrics and workload characteristics in order to optimize the {db_metric} metric.
            Workload and database kernel information: 
            {environment}
            Current Configuration:
            {current_configuration}
            Workload Features: {content}
            Output Format:
            The generated configuration should be formatted as follows:
            - Output must be a valid JSON object.
            - The object must contain:
            - "items": a list of parameter recommendations. Each item must have at least:
                - "name": the parameter name
                - "value": the suggested value
                - "details": (optional) explanation of why this setting is recommended
            - "rationale": a short overall explanation for the recommendations.

            Example:
            {{
            "items": [
                {{"name": "work_mem", "value": "512MB", "details": "Larger work_mem speeds up hash joins"}},
                {{"name": "shared_buffers", "value": "4GB"}}
            ],
            "rationale": "Adjusted memory-related parameters for OLAP workload efficiency."
            }}
            Now, let's think step by step.
        """.format(question_domain = question_domain, db_metric = db_metric, environment=OLAP_environment, content=content, current_configuration=current_configuration)
    else:
        raise NotImplementedError
    return question_analyzer, prompt_get_question_analysis

def get_consensus_prompt(syn_report):
    voter = f"You are an experienced database administrator, skilled in database optimization."
    cons_prompt = """
        Here is a tuning report generated by multiple agents: {syn_report}
        As a experienced database administrator, please carefully review the report and decide whether you agree with its conclusions based on your professional judgment."
        Workload and database kernel information: {OLAP_environment}        
        Output Format Requirements:
        - Output must be a valid JSON object.
        - The object must contain:
        - "opinion": either "Accept" or "Reject", indicating whether you agree with the overall report. If the opinion is "Accept", stop output immediately after producing the JSON object.
        - "Revisions": a list of objects, each describing an agent that needs revision.
            Each item should include:
            - "agent": the name of the agent ("KnobTuner", "IndexRecommender", "MatViewRecommender")
            - "comment": a short explanation or suggestion for improvement.

        Example:
        {{
        "opinion": "Reject",
        "revisions": [
            {{"agent": "KnobTuner", "comment": "Memory parameters are too aggressive for this workload"}},
            {{"agent": "MatViewRecommender", "comment": "Suggested view duplicates an existing index benefit"}}
        ]
        }}
    """.format(syn_report = syn_report, OLAP_environment=OLAP_environment)

    return voter, cons_prompt

def revision_prompt(question_domain, comments, original_recommendation):
    question_analyzer = f"You are an experienced database administrators, skilled in database {question_domain}. " 
    if question_domain == 'materialised views recommendation':
        prompt_get_question_analysis = """
            Task Overview: 
            Revise your previous recommendations on materialised views in order to optimize the {db_metric} metric. 
            Carefully read the ControlAgent’s feedback. Modify your previous recommendations accordingly to address the raised concerns.
            Here is the feedback from the ControlAgent:
            {comments}
            Here is your original recommendation report:
            {original_recommendation}
            Workload and database kernel information: 
            {environment}
            Workload Features: {content}
            Output Format:
            - Output must be a valid JSON object.
            - The object must contain:
            - "items": a list of materialized view recommendations. Each item must have:
                - "name": the materialized view name
                - "query": the SQL query that defines the materialized view
                - "details": (optional) explanation of why this materialized view helps
            - "rationale": a short overall explanation for the recommendations.

            Example:
            {{
            "items": [
                {{"name": "mv_top_customers", "query": "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id", "details": "Precomputes aggregation for top customers"}}
            ],
            "rationale": "Pre-aggregating common queries reduces repeated computation."
            }}
            Now, let's think step by step.
        """.format(comments = comments, original_recommendation = original_recommendation, question_domain = question_domain, db_metric = db_metric, environment=OLAP_environment, content=content)
    elif question_domain == 'indexes recommendation':
        prompt_get_question_analysis = """
            Task Overview: 
            Revise your previous recommendations on indexes in order to optimize the {db_metric} metric. 
            Carefully read the ControlAgent’s feedback. Modify your previous recommendations accordingly to address the raised concerns.
            Here is the feedback from the ControlAgent:
            {comments}
            Here is your original recommendation report:
            {original_recommendation}
            Workload and database kernel information: 
            {environment}
            Workload Features: {content}
            Output Format:
            - Output must be a valid JSON object.
            - The object must contain:
            - "items": a list of index recommendations. Each item must have:
                - "name": the index name
                - "table": the table where the index will be created
                - "columns": the columns to be indexed
                - "details": (optional) explanation of why this index helps
            - "rationale": a short overall explanation for the recommendations.

            Example:
            {{
            "items": [
                {{"name": "idx_orders_customer", "table": "orders", "columns": ["customer_id"], "details": "Speeds up lookups by customer"}}
            ],
            "rationale": "Adding selective indexes reduces scan costs for common queries."
            }}
        """.format(comments = comments, original_recommendation = original_recommendation, question_domain = question_domain, db_metric = db_metric, environment=OLAP_environment, content=content)
    elif question_domain =="knob tuning":
        prompt_get_question_analysis = """
            Task Overview: 
            Revise your previous recommendations on knobs in order to optimize the {db_metric} metric. 
            Carefully read the ControlAgent’s feedback. Modify your previous recommendations accordingly to address the raised concerns.
            Here is the feedback from the ControlAgent:
            {comments}
            Here is your original recommendation report:
            {original_recommendation}
            Workload and database kernel information: 
            {environment}
            Current Configuration:
            {current_configuration}
            Workload Features: {content}
            Output Format:
            The generated configuration should be formatted as follows:
            - Output must be a valid JSON object.
            - The object must contain:
            - "items": a list of parameter recommendations. Each item must have at least:
                - "name": the parameter name
                - "value": the suggested value
                - "details": (optional) explanation of why this setting is recommended
            - "rationale": a short overall explanation for the recommendations.

            Example:
            {{
            "items": [
                {{"name": "work_mem", "value": "512MB", "details": "Larger work_mem speeds up hash joins"}},
                {{"name": "shared_buffers", "value": "4GB"}}
            ],
            "rationale": "Adjusted memory-related parameters for OLAP workload efficiency."
            }}
            Now, let's think step by step.
        """.format(comments = comments, original_recommendation = original_recommendation, question_domain = question_domain, db_metric = db_metric, environment=OLAP_environment, content=content, current_configuration=current_configuration)
    else:
        raise NotImplementedError
    return question_analyzer, prompt_get_question_analysis

def get_consensus_opinion_prompt(domain, syn_report):
    opinion_prompt = f"Here is a tuning report: {syn_report} \n"\
        f"Workload and database kernel information: {OLAP_environment}\n"\
        f"As a experienced database administrator specialized in {domain}, please make full use of your expertise to propose revisions to this report." \
        f"You should output in exactly the same format as '''Revisions: [proposed revision advice] '''"
    return opinion_prompt

def get_search_prompt_auto(domain):
    search_prompt = f"You are an experienced database administrator, skilled in database {domain}. "

    if domain == 'knob tuning':
        domain_context = knob_context
    elif domain == 'indexes recommendation':
        domain_context = index_context
    elif domain == 'materialised views recommendation':
        domain_context = matview_context
    elif domain == 'optimization plan review':
        domain_context = review_context
    else:
        raise NotImplementedError
    
    prompt ="""
    Task Overview:
    You are given a context describing the current tuning scenario. Determine whether the provided context is sufficient to perform effective {domain}.  
    If the context is insufficient, generate concise search keywords that would help find the missing information.
    Note that some items in the current context only contain their names and meanings, their detailed content will be provided later during actual execution.
    Context: {context}
    Output Format:
    - Output must be a valid JSON object.
    - The object must contain:
        - "sufficient": either "True" or "False", indicating whether you think need the search. If the opinion is "True", stop output immediately after producing the JSON object.
        - "keywords": a list of search keywords (only if "sufficient" is "False"). Each keyword should be concise and directly related to the missing information needed for effective {domain}.
    Example:
    {{
        "sufficient": "False",
        "keywords": ["PostgreSQL OLAP performance tuning", "Indexing strategies for OLAP workloads"]
    }}
    """.format(domain = domain, context=domain_context)
    return search_prompt,prompt

def get_search_prompt_on(domain):
    search_prompt = f"You are an experienced database administrator, skilled in database {domain}. "

    if domain == 'knob tuning':
        domain_context = knob_context
    elif domain == 'indexes recommendation':
        domain_context = index_context
    elif domain == 'materialised views recommendation':
        domain_context = matview_context
    elif domain == 'optimization plan review':
        domain_context = review_context
    else:
        raise NotImplementedError
    
    prompt ="""
    Task Overview:
    You are given a context describing the current tuning scenario. 
    Your task is to generate concise and relevant search keywords that would help retrieve any missing information required for effective {domain}.
    Note that some items in the current context only contain their names and meanings, their detailed content will be provided later during actual execution.
    Context: {context}
    Output Format:
    - Output must be a valid JSON object.
    - The object must contain:
        - "keywords": a list of search keywords (only if "sufficient" is "False"). Each keyword should be concise and directly related to the missing information needed for effective {domain}.
    Example:
    {{
        "keywords": ["PostgreSQL OLAP performance tuning", "Indexing strategies for OLAP workloads"]
    }}
    """.format(domain = domain, context=domain_context)
    return search_prompt,prompt
import os
import json
import configparser
import importlib.util
from typing import Dict, Any
