from agents.dem_Agent import DemAgent


if __name__ == "__main__":
    # TODO: Add PLZ-Code as Env-Var
    agent = DemAgent(date='2018-02-05', plz=44)
    agent.connections['mongoDB'].login(agent.name)
    try:
        agent.run()
    except Exception as e:
        print(e)
