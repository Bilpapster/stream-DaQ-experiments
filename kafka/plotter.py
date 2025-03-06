import os
import numpy as np
import matplotlib.pyplot as plt


def read_values_from_txt(file_path):
    with open(file_path, 'r') as file:
        values = [float(line.strip()) for line in file]
    return values

def specify_color(label: str):
    if "pathway" in label:
        return "#1d0996"
    if "faust" in label:
        return "#44d098"
    if "quix" in label:
        return "#b700ff"
    if "bytewax" in label:
        return "#f6bc0f"


def main():
    base_dir = '../framework-comparisons/results'
    data = {}

    for file_name in os.listdir(base_dir):
        if file_name.endswith('.txt'):
            file_path = os.path.join(base_dir, file_name)
            values = read_values_from_txt(file_path)
            mean_value = np.mean(values)
            std_dev = np.std(values)
            data[file_name.replace('executions.txt', '')] = (mean_value, std_dev)

    # Plotting
    labels = list(data.keys())
    means = [data[label][0] for label in labels]
    std_devs = [data[label][1] for label in labels]
    colors = [specify_color(label) for label in labels]

    plt.figure(figsize=(10, 6))
    plt.bar(labels, means, yerr=std_devs, capsize=5, color=colors)
    plt.xlabel('File')
    plt.ylabel('Average Value')
    plt.title('Average Values with Standard Deviation')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()