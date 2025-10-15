import torch
from transformers import AutoTokenizer, BitsAndBytesConfig
from peft import AutoPeftModelForCausalLM
import re

if __name__ == "__main__":
    # Model ID for the Mistral English v2 model
    model_id = "julioc-p/mistral_txt_sparql_en_v2"

    # Configuration for 4-bit quantization

    model = AutoPeftModelForCausalLM.from_pretrained(
        model_id,
        device_map="auto"
    )
    tokenizer = AutoTokenizer.from_pretrained(model_id)

    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
        model.config.pad_token_id = tokenizer.pad_token_id


    # SPARQL extraction function
    def extract_sparql(text):
        code_block_match = re.search(r"```(?:sparql)?\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
        if code_block_match:
            text_to_search = code_block_match.group(1)
        else:
            # v2 models wrap output in ```sparql ... ``` so this is the main path
            text_to_search = text

        match = re.search(r"(SELECT|ASK|CONSTRUCT|DESCRIBE).*?\}", text_to_search, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(0).strip()
        return ""


    question = "Who was Barnard College's American female employee?"
    example_context_json_str = '''
    {
      "entities": {
        "Barnard College": "Q167733",
        "American": "Q30",
        "female": "Q6581072",
        "employee": "Q5"
      },
      "relationships": {
        "instance of": "P31",
        "employer": "P108",
        "gender": "P21",
        "country of citizenship": "P27"
      }
    }
    '''

    system_message_template = """You are an expert text to SparQL query translator. Users will ask you questions in English and you will generate a SparQL query based on the provided context encloses in ```sparql <respose_query>```.
    CONTEXT:
    {context}"""

    # Format the system message with the actual context
    formatted_system_message = system_message_template.format(context=example_context_json_str)

    chat_template = [
        {"role": "system", "content": formatted_system_message},
        {"role": "user", "content": question},
    ]

    inputs = tokenizer.apply_chat_template(
        chat_template,
        tokenize=True,
        add_generation_prompt=True,
        return_tensors="pt"
    ).to(model.device)

    # Generate the output
    with torch.no_grad():
        outputs = model.generate(
            input_ids=inputs.input_ids,
            attention_mask=inputs.attention_mask,
            max_new_tokens=512,
            do_sample=True,
            temperature=0.7,
            top_p=0.9,
            pad_token_id=tokenizer.pad_token_id
        )

    # Decode only the generated part
    generated_text_full = tokenizer.decode(outputs[0], skip_special_tokens=True)
    assistant_response_part = generated_text_full.split("<|im_start|>assistant")[-1].split("<|im_end|>")[0].strip()

    cleaned_sparql = extract_sparql(assistant_response_part)

    print(f"Question: {question}")
    print(f"Context: {example_context_json_str}")
    print(f"Generated SPARQL: {cleaned_sparql}")
    print(f"Assistant's Raw Response: {assistant_response_part}")

