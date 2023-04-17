# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 15:42:30 2023

@author: MC
"""
'''
import os
import openai
openai.organization = "org-5xEtj8oqycN2nDHG3e1a3QpC"
openai.api_key = 'sk-UWCSBt2jr3wZwm2oj04fT3BlbkFJcSaGCa8OXDEAich0QNq'  #os.getenv("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")
openai.Model.list()
'''


import openai
openai.api_key = ('sk-UWCSBt2jr3wZwm2oj04fT3BlbkFJcSaGCa8OXDEAich0QNq')
openai.ChatCompletion.create(
  model="gpt-3.5-turbo",
  messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Who won the world series in 2020?"},
        {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
        {"role": "user", "content": "Where was it played?"}
    ]
)


import openai
openai.api_key = 'sk-UWCSBt2jr3wZwm2oj04fT3BlbkFJcSaGCa8OXDEAich0QNq'
# 通过 `系统(system)` 角色给 `助手(assistant)` 角色赋予一个人设
messages = [{'role': 'system', 'content': '你是一个乐于助人的诗人。'}]
# 在 messages 中加入 `用户(user)` 角色提出第 1 个问题
messages.append({'role': 'user', 'content': '作一首诗，要有风、要有肉，要有火锅、要有雾，要有美女、要有驴！'})
# 调用 API 接口
response = openai.ChatCompletion.create(
    model='gpt-3.5-turbo',
    messages=messages,
)
# 在 messages 中加入 `助手(assistant)` 的回答
messages.append({
    'role': response['choices'][0]['message']['role'],
    'content': response['choices'][0]['message']['content'],
})
# 在 messages 中加入 `用户(user)` 角色提出第 2 个问题
messages.append({'role': 'user', 'content': '好诗！好诗！'})
# 调用 API 接口
response = openai.ChatCompletion.create(
    model='gpt-3.5-turbo',
    messages=messages,
)
# 在 messages 中加入 `助手(assistant)` 的回答
messages.append({
    'role': response['choices'][0]['message']['role'],
    'content': response['choices'][0]['message']['content'],
})
# 查看整个对话
print(messages)



'''
import openai

# 设置 API Key
openai.api_key = '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'


# 调用 OpenAI API
response = openai.Completion.create(
    engine='davinci',
    prompt='Hello, World!',
    max_tokens=5
)

# 输出结果
print(response.choices[0].text)
'''