import http from 'k6/http';
import { check, sleep } from 'k6';

const defaultStages = [
  { duration: '30s', target: 50 },  // Ramp-up to 50 concurrent users
  { duration: '1m', target: 200 },  // Ramp-up to 200 concurrent users
  { duration: '2m', target: 500 },  // Peak sustained load at 500 concurrent users
  { duration: '30s', target: 0 },    // Ramp-down to 0 users
];

const fastStages = [
  { duration: '5s', target: 50 },    // Ramp-up to 50 VUs
  { duration: '20s', target: 100 },  // Peak at 100 VUs
  { duration: '5s', target: 0 },     // Ramp-down
];

const warmupStages = [
  { duration: '30s', target: 50 },  // 30-second unmeasured warm-up
];

let selectedStages = defaultStages;
if (__ENV.PROFILE === 'fast') {
  selectedStages = fastStages;
} else if (__ENV.PROFILE === 'warmup') {
  selectedStages = warmupStages;
}

export const options = {
  stages: selectedStages,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  thresholds: {
    http_req_failed: ['rate<0.05'],
    http_req_duration: ['p(95)<500'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

// Pool of standard merchant category codes
const MCC_LIST = ['5411', '5812', '5311', '5999', '4121'];

// Helper to generate a UUID v4 string
function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export default function () {
  // 80% distributed across 10,000 distinct cards (clean transactions)
  // 20% concentrated across a pool of 20 cards (triggers velocity limit declines)
  const isHotCard = Math.random() < 0.2;
  const cardNum = isHotCard
    ? Math.floor(Math.random() * 20) + 1
    : Math.floor(Math.random() * 10000) + 1;

  const cardId = `card_${cardNum.toString().padStart(6, '0')}`;
  const accountId = `acc_${cardNum.toString().padStart(6, '0')}`;
  const mcc = MCC_LIST[Math.floor(Math.random() * MCC_LIST.length)];
  const amount = (Math.random() * 250 + 1).toFixed(2);

  const payload = JSON.stringify({
    transactionId: generateUUID(),
    cardId: cardId,
    accountId: accountId,
    amount: parseFloat(amount),
    currency: 'USD',
    merchantCategoryCode: mcc,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(`${BASE_URL}/api/v1/fraud/velocity-check`, payload, params);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'valid status reason returned': (r) => {
      try {
        const body = JSON.parse(r.body);
        return (
          body.statusReason === 'APPROVED' ||
          body.statusReason === 'DECLINED_VELOCITY_LIMIT_EXCEEDED' ||
          body.statusReason === 'DECLINED_BLACKLISTED_MERCHANT'
        );
      } catch (e) {
        return false;
      }
    },
  });

  sleep(0.01);
}
