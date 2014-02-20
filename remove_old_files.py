import os
import time

def remove_oldies(folder):
    now = time.time()
    then = now - 60
    for f in os.listdir(folder):
        ts = f.split('_')[0]
        try:
            if float(ts) < then:
                os.remove(os.path.join(folder, f))
        except ValueError:
            pass
    return nuked

if __name__ == "__main__":
    remove_oldies('upload_data')
