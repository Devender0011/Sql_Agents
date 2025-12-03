from build_agent import get_agent

agent, model, db, tools = get_agent()

print("Agent loaded. Ask a question (type 'quit' to exit).")
while True:
    user_q = input("\nQuestion: ").strip()
    if user_q.lower() in ("quit", "exit"):
        break

    
    for step in agent.stream(
        {"messages": [{"role": "user", "content": user_q}]},
        stream_mode="values",
    ):
        if "messages" in step:
            try:
                step["messages"][-1].pretty_print()
            except Exception:
                print(step["messages"][-1])
        elif "__interrupt__" in step:
            print("INTERRUPT:", step["__interrupt__"])
        else:
            
            print(step)