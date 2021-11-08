from agents.pwp_Agent import PwpAgent

if __name__ == "__main__":

    # TODO: Add PLZ-Code as Env-Var
    agent = PwpAgent(date='2018-01-10', plz=50)
    agent.connections['mongoDB'].login(agent.name)
    try:
        agent.run()
    except Exception as e:
        print(e)
    finally:
        agent.connections['mongoDB'].logout(agent.name)
        agent.connections['influxDB'].influx.close()
        agent.connections['mongoDB'].mongo.close()
        if not agent.connections['connectionMQTT'].is_closed:
            agent.connections['connectionMQTT'].close()
        exit()

