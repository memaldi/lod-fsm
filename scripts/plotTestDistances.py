import matplotlib.pyplot as plt

threshold = []
agreement = []

with open('../validation/tesdistances.csv') as f:
    for line in f:
        sline = line.replace('\n', '').split(';')
        threshold.append(float(sline[0].replace(',', '.')))
        agreement.append(float(sline[1].replace(',', '.')))

print threshold
print agreement

fig, ax = plt.subplots()
ax.plot(threshold, agreement, 'b-', label='Agreement')

for t, a in zip(threshold, agreement):
    ax.plot([t], [a], 'bo',)
    plt.annotate(round(a, 2), xy=(t, a), xytext=(t + 0.01, a + 0.01), color='blue')

plt.show()
