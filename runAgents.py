import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import argparse
import subprocess

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, required=False, default=1, help='Start-Point to build Areas')
    parser.add_argument('--end', type=int, required=False, default=2, help='End-Point to build Areas')
    parser.add_argument('--typ', type=str, required=False, default='PWP', help='Agent Typ like RES, PWP or DEM')
    parser.add_argument('--mongo', type=str, required=False, default='149.201.88.150', help='IP MongoDB')
    parser.add_argument('--influx', type=str, required=False, default='149.201.88.150', help='IP InfluxDB')
    parser.add_argument('--market', type=str, required=False, default='149.201.88.150', help='IP Market')
    parser.add_argument('--dbName', type=str, required=False, default='MAS_2020', help='Name der Datenbank')
    return parser.parse_args()

if __name__=="__main__":

    path = os.getcwd()
    args = parse_args()

    for i in range(args.start, args.end +1):
        if args.typ == 'RES':
            subprocess.Popen('python ' + path + r'/dmas/model/agents/res_Agent.py ' + '--plz %i --mongo %s --influx %s --market %s --dbName %s'
                             %(i, args.mongo, args.influx, args.market, args.dbName), cwd=path, shell=True)
        elif args.typ == 'DEM':
            subprocess.Popen('python ' + path + r'/dmas/model/agents/dem_Agent.py ' + '--plz %i --mongo %s --influx %s --market %s --dbName %s'
                             %(i, args.mongo, args.influx, args.market, args.dbName), cwd=path, shell=True)
        elif args.typ == 'PWP':
            subprocess.Popen('python ' + path + r'/dmas/model/agents/pwp_Agent.py ' + '--plz %i --mongo %s --influx %s --market %s --dbName %s'
                             %(i, args.mongo, args.influx, args.market, args.dbName), cwd=path, shell=True)