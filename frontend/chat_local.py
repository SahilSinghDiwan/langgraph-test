from scout.graph import Agent
from scout.prompts import prompts


def main():
    try:
        # A config is required for memory. All graph checkpoints are saved to a thread_id.
        config = {
            "configurable": {
                "thread_id": "1"
            }
        }

        agent = Agent(
            name="Scout",
            system_prompt=prompts.scout_system_prompt,
            model="qwen3:latest",
            temperature=0.1
        )

        # Stream responsesclear
        
        while True:
            user_input = input("User: ")
            if user_input.lower() in ["exit", "quit"]:
                break

            print(f"\n---- User ---- \n\n{user_input}\n")

            print(f"---- Assistant ---- \n")
            # Get the response using our simplified get_stream function
            result = agent.stream(user_input, config=config)

            for message_chunk in result:
                if message_chunk:
                    print(message_chunk, end="", flush=True)

            thread_state = agent.runnable.get_state(config=config)

            if "chart_json" in thread_state.values:
                chart_json = thread_state.values["chart_json"]
                if chart_json:
                    import plotly.io as pio
                    fig = pio.from_json(chart_json)
                    fig.show()
            print("")

    except Exception as e:
        print(f"Error: {type(e).__name__}: {str(e)}")
        raise


if __name__ == "__main__":
    print(f"\nGreetings!\n\nTry asking Scout to show you a preview of the data.\n\n{40*"="}\n\n")

    main()
