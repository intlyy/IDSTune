import os
import json
import configparser
import importlib.util
from typing import Dict, Any
import psycopg2

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
            print(f"Loaded {task_name} features from {file_path}")
    except FileNotFoundError:
        print(f"Warning: Features file not found for task: {task_name}. This will be extracted on first run.")
        print(f"Expected path: {os.path.abspath(file_path)}")

    except FileNotFoundError:
        print(f"File not found for task: {task_name}")

db_metric = "latency"

_PROMPT_DIR = os.path.join(os.path.dirname(__file__), "..", "prompt_template")
_SPECIALIST_PROMPT_PATH = os.path.join(_PROMPT_DIR, "Prompt_Specialist_Agent")
_SUPERVISOR_PROMPT_PATH = os.path.join(_PROMPT_DIR, "Prompt_Supervisor_Agent")


def _load_prompt_json(path: str, label: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read().strip()
    if not raw:
        raise ValueError(f"{label} is empty: {path}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON: {path}") from exc


_SPECIALIST_TEMPLATES = _load_prompt_json(_SPECIALIST_PROMPT_PATH, "Prompt_Specialist_Agent")
_SUPERVISOR_TEMPLATES = _load_prompt_json(_SUPERVISOR_PROMPT_PATH, "Prompt_Supervisor_Agent")


def _get_specialist_template(question_domain: str) -> str:
    try:
        return _SPECIALIST_TEMPLATES[question_domain]
    except KeyError as exc:
        raise NotImplementedError from exc


def _get_supervisor_template() -> str:
    if isinstance(_SUPERVISOR_TEMPLATES, dict) and "consensus" in _SUPERVISOR_TEMPLATES:
        return _SUPERVISOR_TEMPLATES["consensus"]
    raise ValueError("Prompt_Supervisor_Agent must contain a 'consensus' template")

def reset_pgstat_statements():
    conn_str = _build_pg_conn_str()
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    try:
        # Check if pg_stat_statements extension exists
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements';")
        if not cur.fetchone():
            print("Warning: pg_stat_statements extension not installed. Installing...")
            try:
                cur.execute("CREATE EXTENSION pg_stat_statements;")
                conn.commit()
                print("pg_stat_statements extension installed successfully.")
            except Exception as e:
                print(f"Failed to install pg_stat_statements: {e}")
                print("Please manually run: CREATE EXTENSION pg_stat_statements;")
                raise
        cur.execute("SELECT pg_stat_statements_reset();")
        conn.commit()
        print("pg_stat_statements reset successfully.")
    finally:
        cur.close()
        conn.close()

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

def get_question_analysis_prompt(question_domain, search_result="None", current_plan=None):
    question_analyzer = f"You are an experienced database administrators, skilled in database {question_domain}. "
    # Select the appropriate context based on question_domain
    if question_domain == 'knob tuning':
        domain_context = knob_context
    elif question_domain == 'indexes recommendation':
        domain_context = index_context
    elif question_domain == 'materialised views recommendation':
        domain_context = matview_context
    elif question_domain == 'optimization plan review':
        domain_context = review_context
    else:
        raise NotImplementedError

    # Extract domain-specific configuration from current plan
    if current_plan is None:
        domain_config = "Default"
    else:
        if question_domain == 'knob tuning':
            domain_config = json.dumps(current_plan.get('knobs', {}), ensure_ascii=False, indent=2) or "Default"
        elif question_domain == 'indexes recommendation':
            domain_config = json.dumps(current_plan.get('indexes', []), ensure_ascii=False, indent=2) or "Default"
        elif question_domain == 'materialised views recommendation':
            domain_config = json.dumps(current_plan.get('matviews', []), ensure_ascii=False, indent=2) or "Default"
        else:
            domain_config = "Default"

    template = _get_specialist_template(question_domain)
    prompt_get_question_analysis = template.format(
        question_domain=question_domain,
        db_metric=db_metric,
        content=domain_context,
        search_result=search_result,
        current_configuration=domain_config,
    )
    return question_analyzer, prompt_get_question_analysis

def _format_history_for_consensus(history: list) -> str:
    """
    Format historical plans and results for consensus review.
    """
    if not history:
        return "No previous optimization history available."
    
    formatted = []
    for entry in history:
        round_num = entry.get("round", "?")
        result = entry.get("result", "N/A")
        improvement = entry.get("improvement", 0)
        plan = entry.get("plan", {})
        
        formatted.append(
            f"Round {round_num}: Result={result}, Improvement={improvement:.2f}%\n"
            f"Knobs: {json.dumps(plan.get('knobs', {}), ensure_ascii=False)}\n"
            f"Indexes: {len(plan.get('indexes', []))} items\n"
            f"MatViews: {len(plan.get('matviews', []))} items"
        )
    
    return "\n\n".join(formatted)

def get_consensus_prompt(syn_report, search_result="None", current_plan=None, history=None):
    voter = f"You are an experienced database administrator, skilled in database optimization."
    
    # Format history for memory window
    memory_window = _format_history_for_consensus(history)
    
    template = _get_supervisor_template()
    cons_prompt = template.format(
        syn_report=syn_report,
        search_result=search_result,
        content=review_context,
        current_configuration=json.dumps(current_plan, ensure_ascii=False, indent=2) if current_plan else "Default",
        memory_window=memory_window
    )

    return voter, cons_prompt

def revision_prompt(question_domain, comments, original_recommendation, search_result="None", current_plan=None):
    question_analyzer = f"You are an experienced database administrators, skilled in database {question_domain}. "     
    # Select the appropriate context based on question_domain
    if question_domain == 'knob tuning':
        domain_context = knob_context
    elif question_domain == 'indexes recommendation':
        domain_context = index_context
    elif question_domain == 'materialised views recommendation':
        domain_context = matview_context
    elif question_domain == 'optimization plan review':
        domain_context = review_context
    else:
        raise NotImplementedError
    
    # Extract domain-specific configuration from current plan
    if current_plan is None:
        domain_config = "Default"
    else:
        if question_domain == 'knob tuning':
            domain_config = json.dumps(current_plan.get('knobs', {}), ensure_ascii=False, indent=2) or "Default"
        elif question_domain == 'indexes recommendation':
            domain_config = json.dumps(current_plan.get('indexes', []), ensure_ascii=False, indent=2) or "Default"
        elif question_domain == 'materialised views recommendation':
            domain_config = json.dumps(current_plan.get('matviews', []), ensure_ascii=False, indent=2) or "Default"
        else:
            domain_config = "Default"
    if question_domain == 'materialised views recommendation':
        prompt_get_question_analysis = """
            Task Overview: 
            Revise your previous recommendations on materialised views in order to optimize the {db_metric} metric. 
            Carefully read the ControlAgent’s feedback. Modify your previous recommendations accordingly to address the raised concerns.
            Here is the feedback from the ControlAgent:
            {comments}
            Here is your original recommendation report:
            {original_recommendation}
            Workload Features: {content}
            Extra info: {search_result}
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
        """.format(comments = comments, original_recommendation = original_recommendation, question_domain = question_domain, db_metric = db_metric,  content=domain_context, search_result=search_result)
    elif question_domain == 'indexes recommendation':
        prompt_get_question_analysis = """
            Task Overview: 
            Revise your previous recommendations on indexes in order to optimize the {db_metric} metric. 
            Carefully read the ControlAgent’s feedback. Modify your previous recommendations accordingly to address the raised concerns.
            Here is the feedback from the ControlAgent:
            {comments}
            Here is your original recommendation report:
            {original_recommendation}
            Workload Features: {content}
            Extra info: {search_result}
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
        """.format(comments = comments, original_recommendation = original_recommendation, question_domain = question_domain, db_metric = db_metric, content=domain_context, search_result=search_result)
    elif question_domain =="knob tuning":
        prompt_get_question_analysis = """
            Task Overview: 
            Revise your previous recommendations on knobs in order to optimize the {db_metric} metric. 
            Carefully read the ControlAgent’s feedback. Modify your previous recommendations accordingly to address the raised concerns.
            Here is the feedback from the ControlAgent:
            {comments}
            Here is your original recommendation report:
            {original_recommendation}
            Current Configuration:
            {current_configuration}
            Workload Features: {content}
            Extra info: {search_result}
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
        """.format(comments = comments, original_recommendation = original_recommendation, question_domain = question_domain, db_metric = db_metric, content=domain_context, current_configuration=domain_config, search_result=search_result)
    else:
        raise NotImplementedError
    return question_analyzer, prompt_get_question_analysis

def get_consensus_opinion_prompt(domain, syn_report):
    opinion_prompt = f"Here is a tuning report: {syn_report} \n"\
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
    
    prompt = """
    Task Overview:
    You are an expert database tuning agent. You are preparing to tune the system for {domain}.

    Context: 
    {context}

    Goal:
    Determine if you possess sufficient **external domain knowledge** (e.g., best practices, formulas, hardware-specific recommendations, documentation) to tune the items in the context effectively.
    Do NOT worry about specific metric values (e.g., current CPU load), as those will be provided by the system.
    Focus ONLY on whether you need to search for **principles, manuals, or community experiences** regarding the parameters or errors mentioned.

    Output Format:
    Return a strictly valid JSON object.
    {{
        "sufficient": "True" or "False",  // Return "False" if you need to search for external docs/blogs.
        "keywords": ["keyword1", "keyword2"] // Provide 2-3 specific search queries if "sufficient" is "False".
    }}

    Example:
    Context: "Tuning target: explicit_defaults_for_timestamp in MySQL 5.7"
    Output:
    {{
        "sufficient": "False",
        "keywords": ["MySQL 5.7 explicit_defaults_for_timestamp deprecated behavior", "MySQL 5.7 timestamp best practices"]
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
