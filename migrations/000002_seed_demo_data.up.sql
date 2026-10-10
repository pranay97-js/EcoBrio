-- Seed Initial Demo Data matching the EcoBrio Visual Experience Platform

-- 1. Create Core Users
INSERT INTO users (id, email, full_name, role)
VALUES 
    ('11111111-1111-1111-1111-111111111111', 'citizen@ecobrio.org', 'Elena Vance', 'CITIZEN'),
    ('22222222-2222-2222-2222-222222222222', 'collector@ecobrio.org', 'Marcus Thorne', 'COLLECTOR'),
    ('33333333-3333-3333-3333-333333333333', 'admin@ecobrio.org', 'Dr. Sarah Connor', 'ADMIN')
ON CONFLICT (email) DO NOTHING;

-- 2. Create Universal Hubs
INSERT INTO collection_hubs (id, name, address, latitude, longitude, status, operating_hours)
VALUES
    ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'Central Metro Terminal Alpha', 'Platform 4, Transit Boulevard, SW1A 1AA', 51.5074, -0.1278, 'ACTIVE', '06:00 - 22:00 Daily'),
    ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'GreenPark Eco-Depot Beta', '14 Eco-Way, GreenPark District, E1 6AN', 51.5200, -0.0900, 'ACTIVE', '07:00 - 21:00 Daily'),
    ('cccccccc-cccc-cccc-cccc-cccccccccccc', 'TechDistrict Depot Gamma', '8 Silicon Wharf, TechDistrict, EC2A 4NE', 51.5230, -0.0820, 'ACTIVE', '24/7 Autonomous Hopper'),
    ('dddddddd-dddd-dddd-dddd-dddddddddddd', 'Riverside Harbor Point Delta', 'Pier 9, North Riverside Path, SE1 7PB', 51.5030, -0.1130, 'ACTIVE', '06:30 - 20:30 Daily')
ON CONFLICT (id) DO NOTHING;

-- 3. Create Slots for Central Metro Terminal Alpha
INSERT INTO collection_slots (id, hub_id, stream_type, start_time, end_time, total_capacity, booked_count, version)
VALUES
    ('s1111111-1111-1111-1111-111111111111', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'EWASTE', NOW(), NOW() + INTERVAL '2 hour', 10, 0, 1),
    ('s2222222-2222-2222-2222-222222222222', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'PLASTICS', NOW(), NOW() + INTERVAL '2 hour', 20, 5, 1),
    ('s3333333-3333-3333-3333-333333333333', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'METALS', NOW(), NOW() + INTERVAL '2 hour', 15, 2, 1),
    ('s4444444-4444-4444-4444-444444444444', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'ORGANIC', NOW(), NOW() + INTERVAL '2 hour', 25, 8, 1)
ON CONFLICT (id) DO NOTHING;

-- 4. Create Ledger Accounts
INSERT INTO ledger_accounts (id, user_id, code, account_type)
VALUES
    ('99999999-9999-9999-9999-999999999999', NULL, 'SYSTEM_ISSUANCE_RESERVE', 'EQUITY'),
    ('88888888-8888-8888-8888-888888888888', '11111111-1111-1111-1111-111111111111', 'CITIZEN_WALLET:11111111-1111-1111-1111-111111111111', 'LIABILITY')
ON CONFLICT (code) DO NOTHING;
