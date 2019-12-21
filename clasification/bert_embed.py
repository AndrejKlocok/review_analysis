import torch
from pytorch_transformers import BertTokenizer, BertModel, BertForMaskedLM

def main():
    # Load pre-trained model tokenizer (vocabulary)
    modelpath = "bert-base-multilingual-cased"
    tokenizer = BertTokenizer.from_pretrained(modelpath)

    text = "Za tu cenu není co řešit"
    target = "cenu"
    tokenized_text = tokenizer.tokenize(text)

    # Mask a token that we will try to predict back with `BertForMaskedLM`
    masked_index = tokenized_text.index(target)
    tokenized_text[masked_index] = '[MASK]'

    # Convert token to vocabulary indices
    indexed_tokens = tokenizer.convert_tokens_to_ids(tokenized_text)
    # Define sentence A and B indices associated to 1st and 2nd sentences (see paper)
    segments_ids = [1] * len(tokenized_text)
    # this is for the dummy first sentence.
    segments_ids[0] = 0
    segments_ids[1] = 0

    # Convert inputs to PyTorch tensors
    tokens_tensor = torch.tensor([indexed_tokens])
    segments_tensors = torch.tensor([segments_ids])
    # Load pre-trained model (weights)
    model = BertForMaskedLM.from_pretrained(modelpath)
    model.eval()

    # Predict all tokens
    predictions = model(tokens_tensor, segments_tensors)
    t = predictions[0, masked_index]
    predicted_index = torch.argmax(t).item()
    predicted_token = tokenizer.convert_ids_to_tokens([predicted_index])

    print("Original:", text)
    print("Masked:", " ".join(tokenized_text))

    print("Predicted token:", predicted_token)
    print("Other options:")
    # just curious about what the next few options look like.
    for i in range(10):
        predictions[0,masked_index,predicted_index] = -11100000
        predicted_index = torch.argmax(predictions[0, masked_index]).item()
        predicted_token = tokenizer.convert_ids_to_tokens([predicted_index])
        print(predicted_token)


if __name__ == '__main__':
    main()