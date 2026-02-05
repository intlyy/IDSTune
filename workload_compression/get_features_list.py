import json
import os
import configparser
from openai import OpenAI

# Load configuration
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'config.ini'), encoding='utf-8')

# Load prompt templates
prompt_template_path = os.path.join(os.path.dirname(__file__), '..', 'prompt_template', 'Prompt_Feature_Selection')
with open(prompt_template_path, 'r', encoding='utf-8') as f:
    prompt_templates = json.load(f)

messages1 = prompt_templates["system"]
messages2_template = prompt_templates["user"]


def get_features_list(model, messages1, message2, task):
    messages = [  
        {"role": "system", "content": messages1},
        {"role": "user", "content": message2}
    ]
    client = OpenAI(
        api_key=config['LLM']['api_key'], 
        base_url=config['LLM']['base_url']
    )

    completion = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0
    )

    for choice in completion.choices:
        print(choice.message.content)
    
    output_path = os.path.join(os.path.dirname(__file__), f"features_selected_{task}.json")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(choice.message.content)

if __name__ == "__main__":
    model = config['LLM'].get('model', 'gpt-4.1')
    downstream_task = ["indexes recommendation", "materialised views recommendation", "knob tuning", "optimization plan review"]
    
    features_detail_path = os.path.join(os.path.dirname(__file__), "features_detail")
    features_stat_path = os.path.join(os.path.dirname(__file__), "features_stat")
    
    with open(features_detail_path, "r", encoding="utf-8") as f:
        features_all = f.read()
    
    with open(features_stat_path, "r", encoding="utf-8") as f:
        features_all += "\n" + f.read()

    for task in downstream_task:
        message2 = messages2_template.format(downstream_task=task, features_all=features_all)
        get_features_list(model, messages1, message2, task)
