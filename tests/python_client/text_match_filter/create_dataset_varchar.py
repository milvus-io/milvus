import random
import pandas as pd
import numpy as np
import argparse
from faker import Faker
from tqdm import tqdm
fake = Faker()

def generate_random_text(length=1000):
    words = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew", "kiwi", "lemon"]
    words += [fake.word() for _ in range(length)]
    return words

def generate_dataset(size, array_length, hit_probabilities, target_values):
    dataset = []
    all_target_values = set(
        val for sublist in target_values.values() for val in (sublist if isinstance(sublist, list) else [sublist]))
    candidate_values = generate_random_text()
    available_values = [val for val in candidate_values if val not in all_target_values]
    for i in tqdm(range(size), desc="Generating dataset"):
        entry = {"id": i}
        # Generate random arrays for each condition
        for condition in hit_probabilities.keys():
            array = random.sample(available_values, array_length)

            # Ensure the array meets the condition based on its probability
            if random.random() < hit_probabilities[condition]:
                if condition == 'contains':
                    if target_values[condition] not in array:
                        array[random.randint(0, array_length - 1)] = target_values[condition]
                elif condition == 'contains_any':
                    if not any(val in array for val in target_values[condition]):
                        array[random.randint(0, array_length - 1)] = random.choice(target_values[condition])
                elif condition == 'contains_all':
                    indices = random.sample(range(array_length), len(target_values[condition]))
                    for idx, val in zip(indices, target_values[condition]):
                        array[idx] = val
                elif condition == 'equals':
                    array = target_values[condition][:]

            entry[condition] = array

        dataset.append(entry)

    return dataset


def main(data_size, hit_rate=0.005):
    # Parameters
    size = data_size  # Number of arrays in the dataset
    array_length = 10  # Length of each array

    # Probabilities that an array hits the target condition
    hit_probabilities = {
        'contains': hit_rate,
        'contains_any': hit_rate,
        'contains_all': hit_rate,
        'equals': hit_rate
    }

    # Target values for each condition
    target_values = {
        'contains': "apple",
        'contains_any': ["banana", "cherry", "date"],
        'contains_all': ["elderberry", "fig"],
        'equals': ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew", "kiwi", "lemon"]
    }

    # Generate dataset
    dataset = generate_dataset(size, array_length, hit_probabilities, target_values)

    # Define testing conditions
    contains_value = target_values['contains']
    contains_any_values = target_values['contains_any']
    contains_all_values = target_values['contains_all']
    equals_array = target_values['equals']

    # Perform tests
    contains_result = [d for d in dataset if contains_value in d["contains"]]
    contains_any_result = [d for d in dataset if any(val in d["contains_any"] for val in contains_any_values)]
    contains_all_result = [d for d in dataset if all(val in d["contains_all"] for val in contains_all_values)]
    equals_result = [d for d in dataset if d["equals"] == equals_array]

    # Calculate and print proportions
    contains_ratio = len(contains_result) / size
    contains_any_ratio = len(contains_any_result) / size
    contains_all_ratio = len(contains_all_result) / size
    equals_ratio = len(equals_result) / size

    print("\nProportion of arrays that contain the value:", contains_ratio)
    print("Proportion of arrays that contain any of the values:", contains_any_ratio)
    print("Proportion of arrays that contain all of the values:", contains_all_ratio)
    print("Proportion of arrays that equal the target array:", equals_ratio)

    data = {
        "id": pd.Series([x["id"] for x in dataset]),
        "contains": pd.Series([x["contains"] for x in dataset]),
        "contains_any": pd.Series([x["contains_any"] for x in dataset]),
        "contains_all": pd.Series([x["contains_all"] for x in dataset]),
        "equals": pd.Series([x["equals"] for x in dataset]),
        "emb": pd.Series([np.array([random.random() for j in range(32)], dtype=np.dtype("float32")) for _ in
                          range(size)])
    }

    df = pd.DataFrame(data)
    print(df)
    df.to_parquet("train.parquet")

    target_id = {
        "contains": [r["id"] for r in contains_result],
        "contains_any": [r["id"] for r in contains_any_result],
        "contains_all": [r["id"] for r in contains_all_result],
        "equals": [r["id"] for r in equals_result]
    }
    target_id_list = [target_id[key] for key in ["contains", "contains_any", "contains_all", "equals"]]

    query_data = {
        "filter": ["contains", "contains_any", "contains_all", "equals"],
        "value": [["apple"], ["banana", "cherry", "date"], ["elderberry", "fig"], equals_array],
        "target_id": target_id_list
    }
    df = pd.DataFrame(query_data)
    print(df)
    df.to_parquet("test.parquet")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_size", type=int, default=100000)
    parser.add_argument("--hit_rate", type=float, default=0.005)
    args = parser.parse_args()
    datasize = args.data_size
    hit_rate = args.hit_rate
    main(datasize, hit_rate)
