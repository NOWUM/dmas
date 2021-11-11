from agents.pwp_Agent import PwpAgent

if __name__ == "__main__":

    # TODO: Add PLZ-Code as Env-Var
    agent = PwpAgent(date='2018-01-10', plz=50)
    agent.connections['mongoDB'].login(agent.name)
    try:
        agent.run()
    except Exception as e:
        print(e)

