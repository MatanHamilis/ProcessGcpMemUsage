import sys
import json
import matplotlib.pyplot as plt

def main():
    path = sys.argv[1]
    with open(path) as f:
         hist = json.load(f)
    keys = []
    max_vals = []
    avg_vals = []
    for k in hist:
        m = hist[k]["Max"]
        a = hist[k]["Usage"]
        keys.append((int(k), a, m))
    keys = sorted(keys)
    max_vals = [i[2] for i in keys]
    avg_vals = [i[1] for i in keys]
    keys = [i[0] for i in keys]
    max_plot = plt.plot(keys, max_vals,'r', label="Total Available Memory")
    avg_plot = plt.plot(keys, avg_vals, 'b', label="Avg. Used Memory")
    plt.legend(loc="upper left")
    plt.ylabel("Google Memory Unit")
    plt.xlabel("Time Slot (=5Mins)")
    plt.show()

if __name__ == "__main__":
    main()
