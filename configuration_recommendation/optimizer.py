import os
import json
from openai import OpenAI
from prompt_generator import *
import  time
from DB_test import *
from google_search import search_lines
import configparser


config = configparser.ConfigParser()
config.read('../config.ini')

# Resolve repository root (IDSTune/) for consistent file I/O
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

search_mode = config.get('configuration recommender', 'search_mode', fallback='OFF')  # Auto, On, OFF
line_limit = config.getint('configuration recommender', 'line_limit', fallback=20)  # number of lines
# Initialize OpenAI client
client = OpenAI(
            api_key=config['LLM']['api_key'], 
            base_url=config['LLM']['base_url']
        )


def call_llm(prompt1, prompt2, model=config['LLM']['model'], fallback="GPT-4.1"):


    start_time = time.time()
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": prompt1},
            {"role": "user", "content": prompt2}
        ],
        temperature=0
    )
    end_time = time.time()
    elapsed_time = end_time - start_time
    messages=[
        {"role": "system", "content": prompt1},
        {"role": "user", "content": prompt2}
    ]

    history_log_path = os.path.join(ROOT_DIR, 'history', 'log')
    os.makedirs(os.path.dirname(history_log_path), exist_ok=True)
    with open(history_log_path, "a", encoding="utf-8") as f:
        # Save input messages
        f.write("=== Input Messages ===\n")
        f.write(json.dumps(messages, indent=2, ensure_ascii=False))
        f.write("\n\n")

        # Save output
        f.write("=== Output ===\n")
        for choice in response.choices:
            output = choice.message.content
            f.write(output + "\n")
        f.write("\n" + "="*40 + "\n\n")

        # Save call time
        f.write(f"=== LLM Call Time ===\n")
        f.write(f"Elapsed time: {elapsed_time:.2f}s\n")
        f.write("\n" + "="*40 + "\n\n")

    return response.choices[0].message.content


def search_web(domain):
    mode = str(search_mode).strip().lower()
    if mode == "auto":
        prompt1, prompt2 = get_search_prompt_auto(domain)
        raw = call_llm(prompt1, prompt2)
        try:
            result = json.loads(raw)
        except Exception:
            return None
        if str(result.get("sufficient", "True")).lower() == "false":
            search_result = []
            for keyword in result.get("keywords", []):
                try:
                    search_result.append(search_lines(keyword, line_limit))
                except Exception:
                    search_result.append([])
            return search_result
        else:
            return None
    elif mode == "on":
        prompt1, prompt2 = get_search_prompt_on(domain)
        raw = call_llm(prompt1, prompt2)
        try:
            result = json.loads(raw)
        except Exception:
            return None
        search_result = []
        for keyword in result.get("keywords", []):
            try:
                search_result.append(search_lines(keyword, line_limit))
            except Exception:
                search_result.append([])
        return search_result
    else:
        return None
       
def param_tuner(current_plan=None):
    search_result = search_web("knob tuning")
    question_analyzer, prompt_get_question_analysis = get_question_analysis_prompt("knob tuning", search_result, current_plan)
    return safe_parse(call_llm(question_analyzer, prompt_get_question_analysis), "KnobTuner")


def index_recommender(current_plan=None):
    search_result = search_web("indexes recommendation")
    question_analyzer, prompt_get_question_analysis = get_question_analysis_prompt("indexes recommendation", search_result, current_plan)
    return safe_parse(call_llm(question_analyzer, prompt_get_question_analysis), "IndexRecommender")


def matview_recommender(current_plan=None):
    search_result = search_web("materialised views recommendation")
    question_analyzer, prompt_get_question_analysis = get_question_analysis_prompt("materialised views recommendation", search_result, current_plan)
    return safe_parse(call_llm(question_analyzer, prompt_get_question_analysis), "MatViewRecommender")

def param_tuner_revise(comments, original_recommendation, current_plan=None):
    search_result = search_web("knob tuning")
    question_analyzer, prompt_get_question_analysis = revision_prompt("knob tuning", comments, original_recommendation, search_result, current_plan)
    return safe_parse(call_llm(question_analyzer, prompt_get_question_analysis), "KnobTuner")


def index_recommender_revise(comments, original_recommendation, current_plan=None):
    search_result = search_web("indexes recommendation")
    question_analyzer, prompt_get_question_analysis = revision_prompt("indexes recommendation", comments, original_recommendation, search_result, current_plan)
    return safe_parse(call_llm(question_analyzer, prompt_get_question_analysis), "IndexRecommender")


def matview_recommender_revise(comments, original_recommendation, current_plan=None):
    search_result = search_web("materialised views recommendation")
    question_analyzer, prompt_get_question_analysis = revision_prompt("materialised views recommendation", comments, original_recommendation, search_result, current_plan)
    return safe_parse(call_llm(question_analyzer, prompt_get_question_analysis), "MatViewRecommender")

def control_node(plan, current_plan=None, history=None):
    search_result = search_web("optimization plan review")
    voter, cons_prompt = get_consensus_prompt(json.dumps(plan, ensure_ascii=False), search_result, current_plan, history)
    try:
        return json.loads(call_llm(voter, cons_prompt))
    except Exception as e:
        # Fallback: accept plan if it has recommendations
        print(f"Controller LLM call failed: {e}. Using fallback logic.")
        has_recommendations = bool(plan.get("knobs")) or bool(plan.get("indexes")) or bool(plan.get("matviews"))
        return {"opinion": "Accept" if has_recommendations else "Reject", "revisions": []}


def safe_parse(text, agent):
    try:
        j = json.loads(text)
        j["agent"] = agent
        return j
    except Exception:
        return {"agent": agent, "items": [], "rationale": text}


def merge_plan(plan, rec):
    # print(plan)
    # print(rec)
    agent = rec.get("agent")
    if agent == "KnobTuner":
        for it in rec.get("items", []):
            name = it.get("name")
            if name:
                plan["knobs"][name] = {key: it[key] for key in ("value", "details") if key in it}
    elif agent == "IndexRecommender":
        existing = {
            (idx.get("table"), tuple(idx.get("columns", [])))
            for idx in plan["indexes"]
        }
        for it in rec.get("items"):
            key = (it.get("table"), tuple(it.get("columns", [])))
            if key not in existing:
                entry = {
                    key_: it[key_]
                    for key_ in ("name", "table", "columns", "details")
                    if key_ in it
                }
                plan["indexes"].append(entry)
                existing.add(key)

    # Merge materialized view recommendations (deduplicate by query)
    elif agent == "MatViewRecommender":
        existing_queries = {mv.get("query") for mv in plan["matviews"]}
        for it in  rec.get("items"):
            query = it.get("query")
            if query and query not in existing_queries:
                entry = {
                    key_: it[key_]
                    for key_ in ("name", "query", "details")
                    if key_ in it
                }
                plan["matviews"].append(entry)
                existing_queries.add(query)


    plan_log_path = os.path.join(ROOT_DIR, 'history', 'plan')
    os.makedirs(os.path.dirname(plan_log_path), exist_ok=True)
    with open(plan_log_path, "a", encoding="utf-8") as f:
        # Save input messages
        f.write("=== Plan Updated ===\n")
        f.write(json.dumps(plan, indent=2, ensure_ascii=False))
        f.write("\n\n")
    
    # print(plan)
    # exit(0)


def run_framework(max_iters, previous_plan=None, history=None):
    plan = {"knobs": {}, "indexes": [], "matviews": [], "history": []}

    print("Collecting initial recommendations...")
    for func in [param_tuner, index_recommender, matview_recommender]:
        rec = func(previous_plan)  # Use previous round's plan
        print(f"  - {rec['agent']} suggested {len(rec.get('items', []))} items")
        merge_plan(plan, rec)
    #plan = plan_init
    for i in range(max_iters):
        print(f"Iteration {i+1}: controller analyzing plan...")
        decision = control_node(plan, previous_plan, history)
        print(decision)

        if decision.get("opinion") == "Accept":
            print("Plan accepted by controller.")
            return plan

        revisions = decision.get("revisions") or decision.get("Revisions") or []
        for item in revisions:
            print(item)
            agent_name = item.get("agent")
            comment = item.get("comment", "")
            if agent_name == "KnobTuner":
                rec = param_tuner_revise(comment, plan["knobs"], previous_plan) 
            elif agent_name == "IndexRecommender":
                rec = index_recommender_revise(comment, plan["indexes"], previous_plan) 
            elif agent_name == "MatViewRecommender":
                rec = matview_recommender_revise(comment, plan["matviews"], previous_plan) 
            else:
                print("Unknown agent:", item)
                continue
            print(f"  - {rec['agent']} refinement -> {len(rec.get('items', []))} items")
            merge_plan(plan, rec)

    print("Max iterations reached. Returning current plan.")
    return plan


if __name__ == "__main__":

    benchmark = config.get('configuration recommender', 'benchmark', fallback='')  # TPC-C, TPC-DS, Sysbench, JOB
    total_time_limit = config.getint('configuration recommender', 'total_time_limit', fallback=0)  # seconds
    query_dir = config.get('configuration recommender', 'query_dir', fallback=None)
    log_file = config.get('configuration recommender', 'log_file', fallback=None)

    print(f"Starting optimization for benchmark: {benchmark}")
    print(f"Query directory: {query_dir}")
    print(f"Total time limit: {total_time_limit}s, Max iterations per round: {config.getint('configuration recommender', 'max_iterations', fallback=1)}")
    
    # Initialize: reset stats and extract baseline features
    print("\n=== Initialization ===")
    print("Resetting configurations...")
    drop_all_materialized_views()
    reset_indexes_to_original()
    #restore_postgres_config()
    print("Resetting pg_stat_statements...")
    reset_pgstat_statements()
    
    # Run baseline test to populate statistics
    print("Running baseline test...")
    baseline_plan = {"knobs": {}, "indexes": [], "matviews": []}
    if benchmark == "TPC-C":
        baseline_result = test_by_tpcc(baseline_plan)
    elif benchmark == "TPC-DS":
        baseline_result = test_by_tpcds(baseline_plan, query_dir, log_file)
    elif benchmark == "Sysbench":
        baseline_result = test_by_sysbench(baseline_plan, log_file)
    elif benchmark == "JOB":
        baseline_result = test_by_job(baseline_plan, query_dir, log_file)
    else:
        print(f"Unknown benchmark: {benchmark}")
        exit(1)
    
    print(f"Baseline result: {baseline_result}")
    
    # Extract features based on baseline workload statistics
    print("Extracting workload features...")
    refresh_context()
    
    start_time = time.time()
    iteration_count = 0
    previous_plan = None  # First round has no previous plan
    history = []  # Memory window: list of {"round": N, "plan": {...}, "result": X}
    memory_window_size = config.getint('configuration recommender', 'memory_window_size', fallback=3)
    
    while True:
        iteration_count += 1
        print(f"\n=== Optimization Round {iteration_count} ===")
        
        # Generate optimization plan based on current features
        print("Generating optimization plan...")
        final_plan = run_framework(config.getint('configuration recommender', 'max_iterations', fallback=1), previous_plan, history)
        
        print(f"Testing optimized plan (round {iteration_count})...")
        if benchmark == "TPC-C":
            result = test_by_tpcc(final_plan)
        elif benchmark == "TPC-DS":
            result = test_by_tpcds(final_plan, query_dir, log_file)
        elif benchmark == "Sysbench":
            result = test_by_sysbench(final_plan, log_file)
        elif benchmark == "JOB":
            result = test_by_job(final_plan, query_dir, log_file)
        else:
            print("Unknown benchmark:", benchmark)
            break
        
        print(f"Optimization result: {result} (baseline: {baseline_result})")
        improvement = ((baseline_result - result) / baseline_result * 100) if baseline_result > 0 else 0
        print(f"Improvement: {improvement:.2f}%")
        
        # Extract features for next iteration
        print("Refreshing features for next iteration...")
        refresh_context()

        plan_out_path = os.path.join(ROOT_DIR, 'optimization_plan.json')
        with open(plan_out_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(final_plan, ensure_ascii=False, indent=2) + "\n")
        current_time = time.time()
        result_out_path = os.path.join(ROOT_DIR, 'optimization_result.json')
        with open(result_out_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({"result": result, "elapsed": current_time - start_time}, ensure_ascii=False) + "\n")

        # Update previous_plan for next round
        previous_plan = final_plan
        
        # Update history with memory window
        history.append({
            "round": iteration_count,
            "plan": final_plan,
            "result": result,
            "improvement": improvement
        })
        # Keep only the most recent N entries
        if len(history) > memory_window_size:
            history = history[-memory_window_size:]

        if total_time_limit and (current_time - start_time) > total_time_limit:
                print("Time limit exceeded, stopping optimization.")
                break