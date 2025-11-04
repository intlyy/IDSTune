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

search_mode = config['configuration recommender']['search_mode']  # Auto, On, Off
line_limit = int(config['configuration recommender']['line_limit'])  # number of lines
# Initialize OpenAI client
client = OpenAI(
            api_key=config['LLM']['api_key'], 
            base_url=config['LLM']['base_url']
        )


def call_llm(prompt1, prompt2, model=config['LLM']['model']):

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": prompt1},
            {"role": "user", "content": prompt2}
        ],
        temperature=0
    )
    messages=[
        {"role": "system", "content": prompt1},
        {"role": "user", "content": prompt2}
    ]

    with open("history/IDSTune/log", "a", encoding="utf-8") as f:
        # 保存输入 messages
        f.write("=== Input Messages ===\n")
        f.write(json.dumps(messages, indent=2, ensure_ascii=False))
        f.write("\n\n")

        # 保存输出
        f.write("=== Output ===\n")
        for choice in response.choices:
            output = choice.message.content
            f.write(output + "\n")
        f.write("\n" + "="*40 + "\n\n")

    return response.choices[0].message.content


def search_web(domian):
    if search_mode == "Auto":
        prompt1, prompt2 = get_search_prompt_auto(domian)
        result = call_llm(prompt1, prompt2)
        if "Yes" in result["decision"]:
            search_result = []
            for keyword in result["keywords"]:
                search_result.append(search_lines(keyword, line_limit))
            return search_result
        else: 
            return None
    elif search_mode == "On":
        prompt1, prompt2 = get_search_prompt_on(domian)
        result = call_llm(prompt1, prompt2)
        search_result = []
        for keyword in result["keywords"]:
            search_result.append(search_lines(keyword, line_limit))
        return search_result
    else:
        return None
       
def param_tuner():
    search_result = search_web("knob tuning")
    question_analyzer, prompt_get_question_analysis = get_question_analysis_prompt("knob tuning",search_result)
    return safe_parse(call_llm(question_analyzer, prompt_get_question_analysis), "KnobTuner")


def index_recommender():
    search_result = search_web("indexes recommendation")
    question_analyzer, prompt_get_question_analysis = get_question_analysis_prompt("indexes recommendation",search_result)
    return safe_parse(call_llm(question_analyzer, prompt_get_question_analysis), "IndexRecommender")


def matview_recommender():
    search_result = search_web("materialised views recommendation")
    question_analyzer, prompt_get_question_analysis = get_question_analysis_prompt("materialised views recommendation",search_result)
    return safe_parse(call_llm(question_analyzer, prompt_get_question_analysis), "MatViewRecommender")

def param_tuner_revise(comments, original_recommendation):
    search_result = search_web("knob tuning")
    question_analyzer, prompt_get_question_analysis = revision_prompt("knob tuning",comments, original_recommendation, search_result)
    return safe_parse(call_llm(question_analyzer, prompt_get_question_analysis), "KnobTuner")


def index_recommender_revise(comments, original_recommendation):
    search_result = search_web("indexes recommendation")
    question_analyzer, prompt_get_question_analysis = revision_prompt("indexes recommendation",comments, original_recommendation, search_result)
    return safe_parse(call_llm(question_analyzer, prompt_get_question_analysis), "IndexRecommender")


def matview_recommender_revise(comments, original_recommendation):
    search_result = search_web("materialised views recommendation")
    question_analyzer, prompt_get_question_analysis = revision_prompt("materialised views recommendation",comments, original_recommendation, search_result)
    return safe_parse(call_llm(question_analyzer, prompt_get_question_analysis), "MatViewRecommender")

def control_node(plan):
    search_result = search_web("optimization plan review")
    prompt1, prompt2 = get_consensus_prompt(plan,search_result)
    try:
        return json.loads(call_llm(prompt1, prompt2))
    except Exception:
        # Fallback simple heuristic
        print("Controller LLM call failed, using fallback logic.")
        exit(0)
        #accept = bool(plan["params"]) and bool(plan["indexes"])
        #return {"accept": accept, "needs_refinement": [] if accept else ["ParamTuner", "IndexRecommender"], "comment": "fallback"}


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

    # 物化视图推荐结果合并（去重依据: query）
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


    with open("history/IDSTune/plan_3", "a", encoding="utf-8") as f:
        # 保存输入 messages
        f.write("=== Plan Updated ===\n")
        f.write(json.dumps(plan, indent=2, ensure_ascii=False))
        f.write("\n\n")
    
    # print(plan)
    # exit(0)


def run_framework(max_iters):
    plan = {"knobs": {}, "indexes": [], "matviews": [], "history": []}

    print("Collecting initial recommendations...")
    for func in [param_tuner, index_recommender, matview_recommender]:
        rec = func()
        print(f"  - {rec['agent']} suggested {len(rec.get('items', []))} items")
        merge_plan(plan, rec)
    #plan = plan_init
    for i in range(max_iters):
        print(f"Iteration {i+1}: controller analyzing plan...")
        decision = control_node(plan)
        print(decision)

        if decision["opinion"] == "Accept":
            print("Plan accepted by controller.")
            return plan

        for name in decision.get("revisions"):
            print(name)
            if name["agent"] == "KnobTuner":
                rec = param_tuner_revise(name["comment"], plan["knobs"] )
            elif name["agent"] == "IndexRecommender":
                rec = index_recommender_revise(name["comment"], plan["indexes"] )
            elif name["agent"] == "MatViewRecommender":
                rec = matview_recommender_revise(name["comment"], plan["matviews"] )
            else:
                print("Unknown agent:", name)
                continue
            print(f"  - {rec['agent']} refinement -> {len(rec.get('items', []))} items")
            merge_plan(plan, rec)

    print("Max iterations reached. Returning current plan.")
    return plan


if __name__ == "__main__":

    benchmark = config['configuration recommender']['benchmark']  # TPC-C, TPC-DS, Sysbench, JOB
    total_time_limit = config['configuration recommender']['total_time_limit']  # seconds
    query_dir = config['configuration recommender'].get('query_dir', None)
    log_file = config['configuration recommender'].get('log_file', None)

    start_time = time.time() 
    while True:
      final_plan = run_framework(config.getint('configuration recommender', 'max_iterations'))
      if benchmark == "TPC-C":
        result = test_by_tpcc(final_plan)
      elif benchmark == "TPC-DS":
        result = test_by_tpcds(final_plan,query_dir,log_file)
      elif benchmark == "Sysbench":
        result = test_by_sysbench(final_plan,log_file)
      elif benchmark == "JOB":
        result = test_by_job(final_plan,query_dir,log_file)
      else:
        print("Unknown benchmark:", benchmark)
        break
      refresh_context()

      with open("optimization_plan.json", "a", encoding="utf-8") as f:
        f.write(json.dumps(final_plan, indent=2))
      current_time = time.time()
      with open("optimization_result.json","a", encoding="utf-8") as f:
        f.write(json.dumps(result, current_time - start_time))

      if(current_time - start_time) > total_time_limit:
        print("Time limit exceeded, stopping optimization.")
        break
