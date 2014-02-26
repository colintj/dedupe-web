import os
import time

def remove_oldies(folder):
    now = time.time()
    then = now - (60 * 15)
    nuked = []
    for f in os.listdir(folder):
        ts = f.split('_')[0]
        try:
            if float(ts) < then:
                nuked.append(f)
                os.remove(os.path.join(folder, f))
        except ValueError:
            pass
    return nuked

if __name__ == "__main__":
    curdir = os.path.dirname(__file__)
    remove_oldies(os.path.join(curdir, 'upload_data'))
