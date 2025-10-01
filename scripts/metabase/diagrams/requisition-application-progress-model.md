# Requisition Application Progress Data Model

This document describes the data structure for the Requisition Application Progress with Recruiter Names model in Metabase.

**Metabase URL:** https://innosource.metabaseapp.com/model/305-requisition-application-progress

## Entity Relationship Diagram

```mermaid
erDiagram
    portal_clients {
        int id PK
        string name
    }

    portal_requisitions {
        int id PK
        int client_id FK
        string requisition_key
        date fill_by
        boolean use_jakib_ai
    }

    portal_applicants {
        int id PK
        string first_name
        string last_name
        string email
        string phone_number
    }

    portal_users {
        int id PK
        string first_name
        string last_name
    }

    portal_requisition_statistics {
        int requisition_id FK
        int applicant_id FK
        int created_by_recruiter_id FK
        int requisition_statistic_type_id
        timestamp created_at
    }

    portal_applicant_job_listings {
        int requisition_id FK
        int applicant_id FK
        timestamp created_at
    }

    portal_jakib_results {
        int requisition_id FK
        int applicant_id FK
        decimal score
        string conversation_type
        timestamp created_at
    }

    portal_resume_scores {
        int requisition_id FK
        int applicant_id FK
        decimal resume_only_score
        timestamp created_at
    }

    portal_applicant_views {
        int requisition_id FK
        int applicant_id FK
        int recruiter_id FK
        timestamp created_at
    }

    portal_applicant_job_offer_responses {
        int requisition_id FK
        int applicant_id FK
        int job_offer_response_id
        timestamp created_at
    }

    portal_clients ||--o{ portal_requisitions : "has"
    portal_requisitions ||--o{ portal_requisition_statistics : "tracks"
    portal_requisitions ||--o{ portal_applicant_job_listings : "has"
    portal_requisitions ||--o{ portal_jakib_results : "has"
    portal_requisitions ||--o{ portal_resume_scores : "has"
    portal_requisitions ||--o{ portal_applicant_views : "has"
    portal_requisitions ||--o{ portal_applicant_job_offer_responses : "has"
    portal_applicants ||--o{ portal_requisition_statistics : "appears_in"
    portal_applicants ||--o{ portal_applicant_job_listings : "applies_via"
    portal_applicants ||--o{ portal_jakib_results : "scored_by"
    portal_applicants ||--o{ portal_resume_scores : "scored_by"
    portal_applicants ||--o{ portal_applicant_views : "viewed_in"
    portal_applicants ||--o{ portal_applicant_job_offer_responses : "responds_to"
    portal_users ||--o{ portal_applicant_views : "performs"
    portal_users ||--o{ portal_requisition_statistics : "creates"
```

## Data Flow

The model tracks applicant progression through the recruitment pipeline, with recruiter attribution at each stage:

```mermaid
flowchart LR
    A[Application] --> B[Applicant View<br/>by Recruiter]
    B --> C[Phone Screen<br/>by Recruiter]
    C --> D[Inno Interview<br/>by Recruiter]
    D --> E[Client Interview<br/>by Recruiter]
    E --> F[Offer<br/>by Recruiter]
    F --> G{Response}
    G -->|Accepted<br/>by Recruiter| H[Offer Accepted]
    G -->|Rejected<br/>by Recruiter| I[Rejected to Pool]
    H --> J[Hire]
```

## Statistic Type IDs

The `portal_requisition_statistics.requisition_statistic_type_id` field tracks different stages:

- **2** = Phone Screen
- **3** = Interview (Inno)
- **4** = Client Interview
- **5** = Offer
- **7** = Rejected to Regional Pool
- **8** = Offer Accepted

## AI Scoring Categories

Applicants are categorized based on AI scores:

| Category | Jakib Score | Resume Only Score |
|----------|-------------|-------------------|
| GREEN    | > 86        | > 81              |
| YELLOW   | > 67        | > 57              |
| RED      | >= 0        | >= 0              |
| NONE     | null        | null              |

## Key Metrics

The final output includes:
- Client and requisition details
- Applicant information and contact details
- AI scores (Jakib and resume-only)
- AI category and method (MINNIE vs NO AI)
- Date progression through each recruitment stage
- **Recruiter names** associated with each stage:
  - First view recruiter
  - Phone screen recruiter
  - Inno interview recruiter
  - Client interview recruiter
  - Offer recruiter
  - Offer accepted recruiter
  - Rejected recruiter
- Pre-score view flag (whether applicant was viewed before scoring)

## Filters

- Requisitions with `fill_by` year >= 2024
- Statistics with `created_at` year >= 2024
- Hires exclude offer response IDs: 2, 3, 4, 5, 6
