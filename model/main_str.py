from agents.str_Agent import StrAgent

if __name__ == "__main__":

    agent = StrAgent(date='2018-01-01', plz=50)
    agent.connections['mongoDB'].login(agent.name)
    try:
        agent.run()
    except Exception as e:
        print(e)
