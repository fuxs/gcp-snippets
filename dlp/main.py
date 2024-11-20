# Copyright 2024 Michael Bungenstock
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from deidentify import DLPservice
import argparse


def main():
    parser = argparse.ArgumentParser("dlp")
    parser.add_argument(
        "cmd",
        help="The command: enc (default) and dec",
        nargs="?",  # optional
        default="enc",
    )
    parser.add_argument(
        "inputs",
        help="The inputs",
        nargs="+",  # at least one
    )
    parser.add_argument(
        "-s",
        "--surrogate",
        help="Surrogate type will be used for reidentifaction (default is 🔒DLP🔒)",
        default="🔒DLP🔒",
    )
    required = parser.add_argument_group("required named arguments")
    required.add_argument(
        "-k",
        "--key_name",
        help="Qualified name of the key "
        "(projects/[PROJECT]/locations/[LOCATION]/keyRings/[KEY_RING]/cryptoKeys/[KEY_NAME]])",
        required=True,
    )
    required.add_argument(
        "-w",
        "--wrapped_key",
        help="Base64 encoded wrapped encryption key",
        required=True,
    )
    args = parser.parse_args()

    dlps = DLPservice(
        key_name=args.key_name,
        wrapped_key=args.wrapped_key,
        surrogate_type=args.surrogate,
    )
    if args.cmd == "enc":
        for i in args.inputs:
            print(dlps.deidentify_encrypted(i))
    else:
        for i in args.inputs:
            print(dlps.reidentify_encrypted(i))


if __name__ == "__main__":
    main()
