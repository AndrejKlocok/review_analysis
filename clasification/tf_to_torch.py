import torch

from transformers.modeling_bert import BertConfig, BertForPreTraining, load_tf_weights_in_bert


bert_dir='/tmp/xkloco00/multi_cased_L-12_H-768_A-12'

tf_checkpoint_path=bert_dir+'/'+"bert_model.ckpt"
bert_config_file = bert_dir+'/'+"bert_config.json"
pytorch_dump_path=bert_dir+'/'+"pytorch_model.bin"

config = BertConfig.from_json_file(bert_config_file)
print("Building PyTorch model from configuration: {}".format(str(config)))
model = BertForPreTraining(config)

# Load weights from tf checkpoint
load_tf_weights_in_bert(model, config, tf_checkpoint_path)

# Save pytorch-model
print("Save PyTorch model to {}".format(pytorch_dump_path))
torch.save(model.state_dict(), pytorch_dump_path)
