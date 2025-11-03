import json
import os
from openai import OpenAI

messages1 = "You are an experienced database administrator, skilled in database optimization."
messages2 = """
    Task Description:
    You are given a candidate set of features that describe a database workload and
    its environment, including query features, data features, and system features.
    Your task is to select the most relevant features for {downstream_task}.
    Candidate Features:
    {features_all}
    Output Format:
    - Output must be a valid JSON object.
    - Keep only the **feature names**, remove all values.
    Example:
    {
        “Query Features": ["Query count", “Read/Write ratio”, ……],
        “Data Features": [“Data scale”, ……],
        “System Features": [“DB engine”, ……]
    }
    Now, let's think step by step
""".format()




def get_features_list(model,messages1, message2,task):
    messages=[  
            {"role": "system", "content": messages1},
            {"role": "user", "content": message2}
    ]
    client = OpenAI(
        api_key=, 
        base_url=
    )

    completion = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature = 0
    )

    for choice in completion.choices:
        print(choice.message.content)
    with open("features_selected_{task}.json", "w", encoding="utf-8") as f:
        f.write(choice.message.content)

if __name__ == "__main__":
    model = "gpt-4.1"
    downstream_task = ["indexes recommendation", "materialised views recommendation", "knob tuning", "optimization plan review"]
    with open("features_detail", "r", encoding="utf-8") as f:
        features_all = f.read()
    
    with open("features_stat", "r", encoding="utf-8") as f:
        features_all += "\n" + f.read()

    for task in downstream_task:
        message2 = messages2.format(downstream_task=task, features_all=features_all)
        get_features_list(model,messages1, message2,task)