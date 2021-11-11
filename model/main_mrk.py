from agents.mrk_Agent import MarketAgent


if __name__ == "__main__":

    agent = MarketAgent(date='2018-01-01', plz=44)
    agent.connections['mongoDB'].login(agent.name)
    try:
        agent.run()
    except Exception as e:
        print(e)
