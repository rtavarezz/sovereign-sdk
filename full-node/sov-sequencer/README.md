# How to test

1. Run SEQ under branch 'jsonfix': https://github.com/AnomalyFi/nodekit-seq
2. run: ./scripts/run.sh;
3. run: ./scripts/build.sh
4. run: ./build/token-cli key import demo.pk
./build/token-cli chain import-anr
4. run: ./build/token-cli chain watch
5. You should see a screen like below after running the SEQ commands:

database: .token-cli
available chains: 2 excluded: []
0) chainID: Em2pZtHr7rDCzii43an2bBi1M2mTFyLN33QP1Xfjy7BcWtaH9
1) chainID: cKVefMmNPSKmLoshR15Fzxmx52Y5yUSPqWiJsNFUg1WgNQVMX
select chainID: 0

In this case, primary chain id is 'Em2pZtHr7rDCzii43an2bBi1M2mTFyLN33QP1Xfjy7BcWtaH9' since chain id 0 was chosen.
'cKVefMmNPSKmLoshR15Fzxmx52Y5yUSPqWiJsNFUg1WgNQVMX' is secondary chain ID which is the same as the namespace.

6. Input all the data fields into src/lib.rs line 251. As commented on line 276, must submit txs first before fetching back the info.

7. If you do cargo test, an error about dropping a runtime during blocking or async error will occur. This is located in our rust sdk(sync funcs, but async req is causing this i believe). Currently trying to resolve this.

8. After having all info from SEQ, do cargo clean -> cargo build -> cargo test --release -- --nocapture

# Sov-Sequencer

Simple implementation of based sequencer generic over batch builder and DA service.

Exposes 2 RPC methods:

1. `sequencer_acceptTx` where input is supposed to be signed and serialized transaction. This transaction is stored in mempool
2. `sequencer_publishBatch` without any input, which builds the batch using batch builder and publishes it on DA layer.

### Submit transactions
Please see [`demo-rollup` README](../../examples/demo-rollup/README.md#how-to-submit-transactions).

### Publish blob
In order to submit transactions to DA layer, sequencer needs to publish them. This can be done by triggering `publishBatch` endpooint:

```bash
./target/debug/sov-cli publish-batch http://127.0.0.1:12345
```

After some time, processed transaction should appear in logs of running rollup
