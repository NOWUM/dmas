from agents.res_Agent import ResAgent


if __name__ == "__main__":
    # TODO: Add PLZ-Code as Env-Var
    agent = ResAgent(date='2018-01-01', plz=55)
    agent.connections['mongoDB'].login(agent.name)
    try:
        agent.run()
    except Exception as e:
        print(e)
