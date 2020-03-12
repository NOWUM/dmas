import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import argparse
import subprocess


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, required=False, default=1, help='Start-Point to build Areas')
    parser.add_argument('--end', type=int, required=False, default=10, help='End-Point to build Areas')
    parser.add_argument('--typ', type=str, required=False, default='RES', help='Agent Typ like RES, PWP or DEM')

    return parser.parse_args()

def buildRES(start, end):

    path = os.getcwd()
    for i in range(start, end+1):
        print(i)
        try:
            subprocess.Popen(['python3', path + r'\agents\res_Agent.py', '--plz', str(i)],
                             cwd=path, stdout=subprocess.DEVNULL)
        except Exception as e:
            print('Error %s' %e)

def buildDEM(start, end):

    path = os.getcwd()
    for i in range(start, end+1):
        print(i)
        try:
            subprocess.Popen(['python3', path + r'\agents\dem_Agent.py', '--plz', str(i)],
                             cwd=path, stdout=subprocess.DEVNULL)
        except Exception as e:
            print('Error %s' %e)

def buildPWP(start, end):

    path = os.getcwd()
    for i in range(start, end+1):
        print(i)
        try:
            subprocess.Popen(['python3', path + r'\agents\pwp_Agent.py', '--plz', str(i)], cwd=path)
        except Exception as e:
            print('Error %s' %e)

if __name__=="__main__":

    args = parse_args()
    if args.typ == 'RES':
        buildRES(args.start, args.end)
    elif args.typ =='DEM':
        buildDEM(args.start, args.end)
    else:
        buildPWP(args.start, args.end)
