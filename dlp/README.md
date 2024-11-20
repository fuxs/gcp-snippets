# DLP

## Create a key ring and a key (optional)

```bash
gcloud kms keyrings create clouddlp --location global
```

```bash
gcloud kms keys create dlp_deidentify_fpe --location global \
  --keyring clouddlp --purpose encryption 
```

```bash
gcloud kms keys list --location global --keyring clouddlp
```

## Create a 256-bit key

```bash
openssl rand -out "./aes_key.bin" 32
```

## Encrypt the key

```bash
gcloud kms encrypt \
  --location=global  \
  --keyring=clouddlp \
  --key=dlp_deidentify_fpe \
  --plaintext-file=aes_key.bin \
  --ciphertext-file=aes_key.enc
```

Use the following command to create a Base64 encoded version of the encrypted
key:

```bash
base64 -i ./aes_key.enc -w 0; echo
```

## Run the example

### Encrypt names and emails

```bash
python main.py \
  --key_name 'projects/vertexit/locations/global/keyRings/clouddlp/cryptoKeys/dlp_deidentify_fpe' \
  --wrapped_key 'CiQAqP24dM7Roa+hitsaDLMECjqJTZH2oW6U+Y3nwkiVb0XHpm4SSAAjYZDv3BA0qMxfUABiyU+tHxx4r9VM+Nxow+ki+UAxUYz+AgBAIB5tfDsVAzqUgc2BweViD4YUAIOedcIA/CXYLZPJjPmCNQ==' \
  enc "My name is Max Mustermann and my email is max.mustermann@gmail.com"
```

### De-identify the text

```bash
python main.py \
  --key_name 'projects/vertexit/locations/global/keyRings/clouddlp/cryptoKeys/dlp_deidentify_fpe' \
  --wrapped_key 'CiQAqP24dM7Roa+hitsaDLMECjqJTZH2oW6U+Y3nwkiVb0XHpm4SSAAjYZDv3BA0qMxfUABiyU+tHxx4r9VM+Nxow+ki+UAxUYz+AgBAIB5tfDsVAzqUgc2BweViD4YUAIOedcIA/CXYLZPJjPmCNQ==' \
  dec "My name is 🔒DLP🔒(44):AQ3ePunP5DPG0bpR+doVxING2OhjdMlSJfVVEWBCVA== and my email is 🔒DLP🔒(56):AZ4Vf/BGv0Z4D0IFRV3dZXmwuJeleQ3K8vVnp+pF2WBA/FJ5cuL+h1U="
```
