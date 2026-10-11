# Credit Clearinghouse, Double-Entry Ledger & Voucher Saga

**Reference:** `EcoBrio_Blueprint.pdf` Section 8 & 9  
**Guaranteed Invariant:** **Balanced Ledger Invariant** ($\sum \text{Debits} = \sum \text{Credits}$) & Saga Compensating Recovery.

---

## 1. Accounting Invariant: Balanced Double-Entry Postings

Every point transaction must satisfy:
$$\sum_{i=1}^{n} \text{Debit}_i = \sum_{j=1}^{m} \text{Credit}_j$$

### Standard Account Chart
- `SYSTEM_ISSUANCE_RESERVE`: System supply of reward tokens.
- `CITIZEN_WALLET:<uuid>`: Citizen reward points balance.
- `ESCROW_HOLD:<key>`: In-flight perk redemption hold.
- `SYSTEM_REDEEMED_SINK`: Terminal account for successfully burned perk points.

---

## 2. Voucher Saga Workflow with Compensating Reversal

```mermaid
sequenceDiagram
    autonumber
    actor Citizen as Citizen / Mobile
    participant Svc as Credits Service
    participant Ledger as Double-Entry Ledger
    participant Outbox as Transactional Outbox
    participant Provider as External Voucher API

    Citizen->>Svc: POST /api/v1/credits/redeem (PerkName, PointsCost)
    
    rect rgb(30, 40, 55)
        Note over Svc,Ledger: Step 1: Move points to Escrow Hold
        Svc->>Ledger: PostBalancedTransaction(Debit CitizenWallet, Credit EscrowHold)
        Svc->>Outbox: SaveEvent("voucher.escrow_locked")
    end

    alt Voucher Provider Succeeds
        Svc->>Provider: GenerateVoucherCode()
        Provider-->>Svc: Success (VoucherCode: "ECO-PASS-9981")
        rect rgb(30, 55, 40)
            Note over Svc,Ledger: Step 2: Settle Escrow into System Sink
            Svc->>Ledger: PostBalancedTransaction(Debit EscrowHold, Credit SystemSink)
            Svc->>Outbox: SaveEvent("voucher.redeemed")
        end
        Svc-->>Citizen: 200 OK (Voucher Code & New Balance)
    else Voucher Provider Fails (502 / Timeout)
        Svc->>Provider: GenerateVoucherCode()
        Provider-->>Svc: Error (HTTP 502 / Connection Timeout)
        rect rgb(55, 30, 30)
            Note over Svc,Ledger: Step 3: Compensating Reversal Saga
            Svc->>Ledger: PostBalancedTransaction(Debit EscrowHold, Credit CitizenWallet)
            Svc->>Outbox: SaveEvent("voucher.compensation_reversed")
        end
        Svc-->>Citizen: 502 Bad Gateway ("Provider failed; points held in escrow restored to wallet")
    end
```
