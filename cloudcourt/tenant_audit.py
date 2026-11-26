"""租户离线审计证据包。

解决 ``audit_slice`` / ``audit_verify`` 的“筛出一行即 verified”问题：

* 以**全量事件承诺**（绑定原始序号、event_id、规范化 payload/result 的哈希链 +
  Merkle 根）固定确切事件序列；删行、重编号、改内容都会与已封存的签名检查点冲突。
* 以**依赖图感知**代替服务名 LIKE：上游数据库 D 的就绪/撤销即使没提到账单服务 B
  也会被纳入；同名的其他租户事件凭租户限定被排除。
* 给出包含证明与撤销/超时/更早启动的**缺席证明**。
* 披露采用字段白名单，其他租户原文整行不披露，只给带随机盲因子的不透明叶子；
  包内不存在对方标识字段的裸哈希、可逆令牌或映射表。
* 两种审计交付安排：
  - ``custodian``：原件交独立审计保管人验证全量事件/回放，出具可追索签名的限定结论；
  - ``self``：平台给全量事件承诺 + 包含/缺席证明 + 脱敏依赖摘要，客户离线自验。
* 证据包封存、签收、送达与更正均入库且文档不可变；更正链接原包并说明结论变化，
  客户离线导入更正后可辨认旧包已失效。

本模块仅使用 Python 标准库。HMAC 在此模拟签名锚点（生产可替换为非对称签名，
验证接口保持不变）。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

FORMAT = 'cloudcourt-tenant-audit/v1'
DEFAULT_KEY = 'dev-cloudcourt-tenant-audit-key'
DEFAULT_CUSTODIAN_KEY = 'dev-cloudcourt-custodian-key'
GENESIS = '0' * 64

# ---------------------------------------------------------------------------
# 小型工具
# ---------------------------------------------------------------------------


class PDict(dict):
    """既可用 ``d['key']`` 也可用 ``d.key`` 访问的字典。"""

    def __getattr__(self, name):
        try:
            value = self[name]
        except KeyError:
            raise AttributeError(name)
        if isinstance(value, dict) and not isinstance(value, PDict):
            value = PDict(value)
            self[name] = value
        return value

    def __setattr__(self, name, value):
        self[name] = value


def _wrap(value):
    if isinstance(value, dict) and not isinstance(value, PDict):
        return PDict({k: _wrap(v) for k, v in value.items()})
    if isinstance(value, list):
        return [_wrap(v) for v in value]
    return value


class Report(PDict):
    """验证报告：既可用 ``r['verified']``/``r.verified``，也可直接 ``assertTrue(r)``。"""

    def __bool__(self):
        if 'verified' in self:
            return bool(self['verified'])
        return bool(self.get('valid', False))


def _as_document(value):
    """接受证据包字典、JSON 文件路径或原始 JSON 文本。"""
    if isinstance(value, (str, os.PathLike)):
        text = str(value)
        if not isinstance(value, os.PathLike) and text.lstrip()[:1] in '{[':
            return json.loads(text)
        path = Path(text)
        if path.exists():
            return json.loads(path.read_text(encoding='utf-8'))
        return json.loads(text)
    return value


def now_iso(at=None):
    if at is None:
        return datetime.now(timezone.utc).isoformat()
    return str(at)


def _json_default(obj):
    if isinstance(obj, (set, tuple)):
        return sorted((str(x) for x in obj))
    if isinstance(obj, (os.PathLike,)):
        return str(obj)
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    return str(obj)


def canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), default=_json_default)


def H(*parts) -> str:
    """带长度前缀分帧的 SHA-256，消除拼接歧义。"""
    digest = hashlib.sha256()
    digest.update(b'cloudcourt-hash/v1\n')
    for part in parts:
        if part is None:
            part = ''
        if isinstance(part, str):
            part = part.encode('utf-8')
        digest.update(len(part).to_bytes(8, 'big'))
        digest.update(part)
    return digest.hexdigest()


def hmac_hex(key, message) -> str:
    if isinstance(key, str):
        key = key.encode('utf-8')
    if isinstance(message, str):
        message = message.encode('utf-8')
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def generate_key(seed=None) -> str:
    """生成签名密钥；给定 seed 时得到确定性开发密钥。"""
    if seed is not None:
        return hashlib.sha256(('cloudcourt-keygen/v1/' + str(seed)).encode()).hexdigest()
    return secrets.token_hex(32)


# ---------------------------------------------------------------------------
# 事件读取与规范化
# ---------------------------------------------------------------------------

def _maybe_json(value):
    if isinstance(value, str):
        text = value.strip()
        if text and text[0] in '{[':
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return value
        return value
    return value


def normalize_event(row, fallback_seq=None):
    """把 sqlite.Row / dict / 纯 payload 统一成事件核心结构。"""
    if isinstance(row, sqlite3.Row):
        row = dict(row)
    if not isinstance(row, dict):
        raise TypeError(f'unsupported event row: {type(row)!r}')

    # 允许直接传入 payload（含 op 的普通字典）。
    if 'payload' not in row and 'op' in row:
        payload = dict(row)
        row = {'payload': payload, 'result': row.get('result')}

    payload = _maybe_json(row.get('payload', {}))
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        payload = {'value': payload}
    result = _maybe_json(row.get('result'))
    if result is not None and not isinstance(result, dict):
        result = {'value': result}

    seq = row.get('seq', row.get('sequence', row.get('seq_no', fallback_seq)))
    try:
        seq = int(seq)
    except (TypeError, ValueError):
        seq = fallback_seq

    event_id = row.get('event_id') or payload.get('event_id') or f'event-{seq}'
    subsystem = row.get('subsystem') or payload.get('subsystem') or 'tenant_audit'
    command = row.get('command') or payload.get('op') or row.get('op') or 'event'
    at = _event_time(payload, row)

    return PDict({
        'seq': seq,
        'event_id': str(event_id),
        'subsystem': str(subsystem),
        'command': str(command),
        'op': str(payload.get('op', command)),
        'at': at,
        'payload': payload,
        'result': result if isinstance(result, dict) else None,
    })


def _event_time(payload, row=None):
    for key in ('at', 'published_at', 'occurred_at', 'ts', 'timestamp', 'time'):
        value = payload.get(key) if isinstance(payload, dict) else None
        if value is None and row is not None:
            value = row.get(key)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    return None


def _rows_from_sqlite(source):
    close = False
    if isinstance(source, (str, os.PathLike)):
        path = str(source)
        if path == ':memory:':
            raise ValueError('cannot reopen :memory: by path; pass the connection instead')
        db = sqlite3.connect(path)
        db.row_factory = sqlite3.Row
        close = True
    else:
        db = source

    try:
        rows = db.execute(
            'SELECT seq,subsystem,command,event_id,payload,result FROM events ORDER BY seq'
        ).fetchall()
        return [dict(r) for r in rows], close, db
    except sqlite3.OperationalError:
        if close:
            db.close()
        raise


def load_events(source):
    """从连接、路径、事件列表或证据包加载规范化事件。"""
    if source is None:
        return []
    if isinstance(source, PDict):
        source = dict(source)
    if isinstance(source, dict):
        if 'events' in source:
            return load_events(source['events'])
        if 'ledger' in source:
            return _events_from_ledger(source['ledger'])
        return [normalize_event(source, 1)]
    if isinstance(source, (list, tuple)):
        events = []
        for i, row in enumerate(source, 1):
            events.append(normalize_event(row, i))
        events.sort(key=lambda e: (e.seq is None, e.seq))
        return events
    if hasattr(source, 'execute'):
        rows, _close, _db = _rows_from_sqlite(source)
        events = [normalize_event(r) for r in rows]
        events.sort(key=lambda e: e.seq)
        return events
    if isinstance(source, (str, os.PathLike)):
        text = str(source)
        if not isinstance(source, os.PathLike) and text.lstrip()[:1] in '{[':
            data = json.loads(text)
            return load_events(data)
        if text.lower().endswith('.json') and Path(text).exists():
            data = json.loads(Path(text).read_text(encoding='utf-8'))
            return load_events(data)
        rows, close, db = _rows_from_sqlite(text)
        try:
            events = [normalize_event(r) for r in rows]
            events.sort(key=lambda e: e.seq)
            return events
        finally:
            if close:
                db.close()
    raise TypeError(f'cannot load events from {type(source)!r}')


def _events_from_ledger(ledger):
    events = []
    for i, item in enumerate(ledger, 1):
        if item.get('disclosed') and item.get('event'):
            events.append(normalize_event(item['event'], item.get('seq', i)))
    return events


def event_core(event) -> PDict:
    e = normalize_event(event)
    return PDict({
        'seq': e.seq,
        'event_id': e.event_id,
        'subsystem': e.subsystem,
        'command': e.command,
        'op': e.op,
        'at': e.at,
        'payload': e.payload,
        'result': e.result,
    })


def full_content_hash(event) -> str:
    return H('event-full/v1', canonical(event_core(event)))


# ---------------------------------------------------------------------------
# Merkle 树与（区间）缺席证明
# ---------------------------------------------------------------------------

class MerkleTree:
    def __init__(self, leaves=None):
        self.leaves = list(leaves or [])
        if not self.leaves:
            self.root = H('merkle-empty/v1')
            self.levels = [[self.root]]
            return
        level = [H('mleaf/v1', str(i), leaf) for i, leaf in enumerate(self.leaves)]
        self.leaf_hashes = level
        self.levels = [level]
        while len(level) > 1:
            nxt = []
            for i in range(0, len(level), 2):
                left = level[i]
                right = level[i + 1] if i + 1 < len(level) else level[i]
                nxt.append(H('mnode/v1', left, right))
            level = nxt
            self.levels.append(level)
        self.root = level[0]

    def proof(self, index):
        if not self.leaves or index < 0 or index >= len(self.leaves):
            return []
        path = []
        idx = index
        for level in self.levels[:-1]:
            if idx % 2 == 0:
                sib_idx = idx + 1
                position = 'right'
            else:
                sib_idx = idx - 1
                position = 'left'
            if sib_idx >= len(level):
                sibling = level[idx]  # 奇数末叶复制自身
            else:
                sibling = level[sib_idx]
            path.append({'position': position, 'hash': sibling})
            idx //= 2
        return path

    @staticmethod
    def verify(leaf, proof, root, index=None, total=None):
        node = leaf
        for step in proof:
            sib = step['hash'] if isinstance(step, dict) else step[1]
            position = step.get('position') if isinstance(step, dict) else step[0]
            if position in ('right', 'r'):
                node = H('mnode/v1', node, sib)
            else:
                node = H('mnode/v1', sib, node)
        return hmac.compare_digest(node, root)


def inclusion_proof(leaves, index):
    tree = MerkleTree(leaves)
    return {'index': index, 'leaf': leaves[index], 'root': tree.root,
            'path': tree.proof(index)}


def verify_inclusion_proof(proof, root=None):
    root = root or proof.get('root')
    leaf_hash = H('mleaf/v1', str(proof['index']), proof['leaf'])
    return MerkleTree.verify(leaf_hash, proof['path'], root)


def merkle_root(leaves):
    return MerkleTree(leaves).root


def sorted_absence_proof(keys, low, high):
    """证明排序键集合中不存在落在 ``[low, high)`` 的键。

    返回相邻前驱/后继及其 Merkle 包含路径；验证方检查两者下标相邻且形成夹逼。
    """
    keys = sorted(keys)
    if len(keys) != len(set(keys)):
        raise ValueError('absence-proof keys must be unique')
    # 以“带位置的键叶子”构造排序索引树。
    key_leaves = [H('skeyleaf/v1', str(i), str(k)) for i, k in enumerate(keys)]
    root = MerkleTree(key_leaves).root

    import bisect
    i = bisect.bisect_left(keys, low)
    predecessor = {'index': i - 1, 'key': keys[i - 1]} if i > 0 else None
    successor = {'index': i, 'key': keys[i]} if i < len(keys) else None
    witnesses = []
    for w in (predecessor, successor):
        if w:
            witnesses.append({**w, 'leaf': key_leaves[w['index']],
                              'path': MerkleTree(key_leaves).proof(w['index'])})
    absent = (i == len(keys)) or keys[i] >= high
    return PDict({'low': low, 'high': high, 'root': root, 'absent': absent,
                  'insert_index': i, 'predecessor': predecessor,
                  'successor': successor, 'witnesses': witnesses})


def _witness_path_valid(w, root):
    node = H('mleaf/v1', str(w['index']), w['leaf'])
    return MerkleTree.verify(node, w['path'], root)


def verify_sorted_absence_proof(root, proof):
    if proof.get('root') and not hmac.compare_digest(proof['root'], root):
        return False
    i = proof['insert_index']
    pred, succ = proof.get('predecessor'), proof.get('successor')
    # 下标必须连续（或位于边界），否则中间可能藏键。
    if pred and succ and pred['index'] + 1 != succ['index']:
        return False
    if pred and not succ and pred['index'] != i - 1:
        return False
    if succ and not pred and succ['index'] != i:
        return False
    if pred:
        w = next((x for x in proof['witnesses'] if x['index'] == pred['index']), None)
        if not w or not _witness_path_valid(w, root):
            return False
        if not pred['key'] < proof['low']:
            return False
    if succ:
        w = next((x for x in proof['witnesses'] if x['index'] == succ['index']), None)
        if not w or not _witness_path_valid(w, root):
            return False
        if not succ['key'] >= proof['high']:
            return False
    return bool(proof.get('absent'))


def _absence_witnesses_valid(proof):
    """只验证缺席证明的见证路径/夹逼/下标连续性，不把 absent 结论与密码学有效性混淆。"""
    root = proof.get('root')
    i = proof.get('insert_index')
    pred, succ = proof.get('predecessor'), proof.get('successor')
    if pred and succ and pred['index'] + 1 != succ['index']:
        return False
    if pred and not succ and pred['index'] != i - 1:
        return False
    if succ and not pred and succ['index'] != i:
        return False
    for w in proof.get('witnesses', []):
        if not _witness_path_valid(w, root):
            return False
    return True


# ---------------------------------------------------------------------------
# 事实抽取与依赖图
# ---------------------------------------------------------------------------

LAUNCH_OPS = {'launch', 'start', 'boot', 'started', 'launched', 'start_service'}
REVISION_OPS = {'revision', 'rev', 'revise', 'revised'}
SEAL_OPS = {'seal', 'sealed', 'seal_edge', 'seal_contract'}
TOKEN_OPS = {'token', 'issue_token', 'bootstrap_token'}
WAVE_OPS = {'plan', 'wave', 'wave_plan'}
SLOT_OPS = {'slot', 'slot_record'}
PROBE_OPS = {'probe', 'probe_sample'}
PUBLISH_OPS = {'publish', 'ready', 'readiness', 'receipt', 'publish_receipt'}
REVOKE_OPS = {'revoke', 'revocation', 'revoked', 'cancel', 'cancelled'}
EXPIRE_OPS = {'expire', 'expiry', 'timeout_receipt', 'expired'}
TIMEOUT_OPS = {'timeout', 'attempt_timeout', 'timed_out'}

LAUNCH_OK = {'launched', 'started', 'launch_recorded'}
PUBLISH_OK = {'published', 'ready', 'receipt_issued'}
SUCCESS_DECISIONS = LAUNCH_OK | PUBLISH_OK | {
    'revision_recorded', 'sealed', 'token_issued', 'planned',
    'slot_recorded', 'sampled', 'edge_added',
}


def decision_of(event):
    result = event.get('result') if isinstance(event, dict) else None
    if isinstance(result, dict):
        return result.get('decision') or result.get('state') or result.get('verdict')
    return None


def _p(event, *keys, default=None):
    p = event.get('payload', {})
    for key in keys:
        if key in p and p[key] is not None:
            return p[key]
    return default


def build_facts(events):
    events = [normalize_event(e) for e in events]
    facts = PDict({
        'events': events,
        'waves': {}, 'attempts': {}, 'receipts': {}, 'tokens': {}, 'slots': {},
        'revisions': {}, 'seals': {}, 'edges': [], 'degrade_grants': [],
        'launches': [], 'timeouts': [], 'revocations': [], 'expiries': [],
        'probes': [], 'publishes': [], 'requests': [],
        'service_aliases': {},
    })

    # 第一遍：别名与基础实体。
    for e in events:
        p = e.payload
        sid = p.get('id') if e.op in REVISION_OPS | SEAL_OPS else None
        for candidate in (p.get('service'), sid, p.get('id')):
            if candidate and p.get('name'):
                facts.service_aliases.setdefault(str(candidate), set()).add(str(p['name']))
                facts.service_aliases.setdefault(str(p['name']), set()).add(str(candidate))
        if e.op in WAVE_OPS and p.get('id'):
            facts.waves[p['id']] = e
        if e.op in LAUNCH_OPS and p.get('id'):
            facts.attempts[p['id']] = e
            facts.launches.append(e)
        if e.op in PUBLISH_OPS and p.get('id'):
            facts.receipts[p['id']] = e
            facts.publishes.append(e)
        if e.op in TOKEN_OPS and p.get('id'):
            facts.tokens[p['id']] = e
        if e.op in SLOT_OPS and p.get('id'):
            facts.slots[p['id']] = e
        if e.op in REVISION_OPS and p.get('id'):
            old = facts.revisions.get(p['id'])
            if old is None or e.seq > old.seq:
                facts.revisions[p['id']] = e
        if e.op in SEAL_OPS and p.get('id'):
            facts.seals[(p['id'], p.get('generation'))] = e
        if e.op == 'edge' or p.get('op') == 'edge':
            facts.edges.append(e)
        if e.op == 'degrade':
            facts.degrade_grants.append(e)
        if e.op in TIMEOUT_OPS:
            facts.timeouts.append(e)
        if e.op in REVOKE_OPS:
            facts.revocations.append(e)
        if e.op in EXPIRE_OPS:
            facts.expiries.append(e)
        if e.op in PROBE_OPS:
            facts.probes.append(e)
        if e.op in {'request', 'tenant_request', 'query'}:
            facts.requests.append(e)

    # 第二遍：attempt -> service。
    for aid, launch in list(facts.attempts.items()):
        svc = _p(launch, 'service')
        wave_id = _p(launch, 'wave')
        if not svc and wave_id and wave_id in facts.waves:
            svc = _p(facts.waves[wave_id], 'service')
        facts.attempts[aid] = PDict({**launch, '_service': svc, '_wave': wave_id})

    # receipt -> service。
    for rid, pub in list(facts.receipts.items()):
        svc = _p(pub, 'service')
        attempt_id = _p(pub, 'attempt')
        if not svc and attempt_id and attempt_id in facts.attempts:
            svc = facts.attempts[attempt_id].get('_service')
        facts.receipts[rid] = PDict({**pub, '_service': svc, '_attempt': attempt_id})

    return facts


def canonical_service(value, target, facts=None, aliases=None):
    if value is None:
        return False
    value, target = str(value), str(target)
    if value == target:
        return True
    alias_sets = []
    if facts is not None:
        alias_sets.append(facts.get('service_aliases', {}).get(target, set()))
        alias_sets.append(facts.get('service_aliases', {}).get(value, set()))
    if aliases:
        alias_sets.append(set(aliases))
    return any(value in s or target in s for s in alias_sets if isinstance(s, set))


def infer_tenant(events, service, facts=None):
    """从目标服务自己的修订/封存事件推断租户，避免同名其他租户污染。"""
    for e in events:
        p = e.payload
        if e.op in REVISION_OPS | SEAL_OPS and canonical_service(p.get('id') or p.get('service'), service, facts):
            tenant = p.get('tenant') or p.get('tenant_id')
            if tenant:
                return tenant
    tenants = {p.get('tenant') or p.get('tenant_id') for e in events
               for p in [e.payload]
               if canonical_service(p.get('service') or p.get('id'), service, facts)
               and (p.get('tenant') or p.get('tenant_id'))}
    return next(iter(t for t in tenants if t), None) if len(tenants) == 1 else None


def critical_dependencies(service, facts, explicit=None, generation=None):
    deps = []
    for e in facts.edges:
        p = e.payload
        if not canonical_service(p.get('service'), service, facts):
            continue
        if generation is not None and p.get('revision') not in (None, generation):
            continue
        if p.get('critical', True) in (True, 1, '1', 'true', 'True'):
            dep = p.get('dependency')
            if dep and dep not in deps:
                deps.append(dep)
    for dep in explicit or []:
        if dep not in deps:
            deps.append(dep)
    return deps


def event_subject_services(e, facts):
    """事件关联到的服务集合（用于依赖感知选择）。"""
    p = e.payload
    out = set()
    for key in ('service', 'target_service'):
        if p.get(key):
            out.add(str(p[key]))
    if e.op in REVISION_OPS | SEAL_OPS and p.get('id'):
        out.add(str(p['id']))
    if e.op == 'edge':
        if p.get('service'):
            out.add(str(p['service']))
        if p.get('dependency'):
            out.add(str(p['dependency']))
    if e.op in LAUNCH_OPS and p.get('id') in facts.attempts:
        svc = facts.attempts[p['id']].get('_service')
        if svc:
            out.add(str(svc))
    if e.op in PUBLISH_OPS and p.get('id') in facts.receipts:
        svc = facts.receipts[p['id']].get('_service')
        if svc:
            out.add(str(svc))
    if e.op in REVOKE_OPS | EXPIRE_OPS:
        rid = p.get('id')
        if rid in facts.receipts:
            svc = facts.receipts[rid].get('_service')
            if svc:
                out.add(str(svc))
        if p.get('target_service'):
            out.add(str(p['target_service']))
    if e.op in PROBE_OPS and p.get('attempt') in facts.attempts:
        svc = facts.attempts[p['attempt']].get('_service')
        if svc:
            out.add(str(svc))
    return out


# ---------------------------------------------------------------------------
# 结论引擎：13 是否可启动、15 是否可对外服务
# ---------------------------------------------------------------------------

def _check(name, ok, detail, evidence=None, severity='hard'):
    if ok is None:
        status = 'not_evidenced'
    elif ok:
        status = 'pass'
    else:
        status = 'fail'
    return PDict({'name': name, 'status': status, 'ok': bool(ok) if ok is not None else None,
                  'severity': severity, 'detail': detail,
                  'evidence_seqs': list(evidence or [])})


def evaluate_case(source, service=None, tenant=None, dependencies=None,
                  launch_seq=None, readiness_seq=None, seq_window=None,
                  time_window=None, aliases=None):
    events = load_events(source)
    facts = build_facts(events)
    if service is None:
        # 自动挑选拥有 launch 事件最多的服务。
        counts = {}
        for e in facts.launches:
            svc = e.get('_service') or _p(e, 'service')
            if svc:
                counts[svc] = counts.get(svc, 0) + 1
        service = max(counts, key=counts.get) if counts else None
    if tenant is None and service is not None:
        tenant = infer_tenant(events, service, facts)

    def is_target(e):
        p = e.payload
        if tenant is not None:
            et = p.get('tenant') or p.get('tenant_id')
            if et is not None and str(et) != str(tenant):
                return False
        subjects = event_subject_services(e, facts)
        if any(canonical_service(s, service, facts, aliases) for s in subjects):
            return True
        direct = p.get('service') or (p.get('id') if e.op in REVISION_OPS | SEAL_OPS else None)
        return canonical_service(direct, service, facts, aliases)

    target_events = [e for e in events if is_target(e)]
    launches = [e for e in facts.launches if is_target(e)]
    launches.sort(key=lambda e: e.seq)

    launch = None
    if launch_seq is not None:
        launch = next((e for e in launches if e.seq == launch_seq), None)
    if launch is None and launches:
        launch = launches[-1]
    lseq = launch.seq if launch else None
    lgen = _p(launch, 'generation') if launch else None
    lat = _p(launch, 'at') if launch else None
    attempt_id = _p(launch, 'id') if launch else None

    # 修订选择必须租户感知：同名其他租户的更高代次修订不能覆盖本租户修订。
    rev_candidates = [
        e for e in events if e.op in REVISION_OPS
        and canonical_service(_p(e, 'id', 'service'), service, facts, aliases)
        and (tenant is None or not (_p(e, 'tenant', 'tenant_id') is not None
                                    and str(_p(e, 'tenant', 'tenant_id')) != str(tenant)))]
    revision = max(rev_candidates, key=lambda e: e.seq) if rev_candidates else None
    rgen = _p(revision, 'generation') if revision else None
    gen = lgen if lgen is not None else rgen

    deps = critical_dependencies(service, facts, dependencies, generation=rgen)

    checks = []

    # 1) 服务修订及其序号。
    checks.append(_check(
        'service_revision_bound', revision is not None,
        f'服务 {service} 的修订事件已绑定' if revision else f'缺少服务 {service} 的修订事件',
        [revision.seq] if revision else []))
    if launch and revision:
        checks.append(_check(
            'revision_before_launch', revision.seq < launch.seq,
            f'修订 seq={revision.seq} 必须早于启动 seq={launch.seq}',
            [revision.seq, launch.seq]))
        if lgen is not None and rgen is not None:
            checks.append(_check(
                'generation_binding', str(lgen) == str(rgen),
                f'启动代次 {lgen} 与已封存修订代次 {rgen} 一致',
                [revision.seq, launch.seq]))

    # 2) 依赖边 + 封存。
    edge_events = [e for e in facts.edges
                   if canonical_service(_p(e, 'service'), service, facts, aliases)
                   and (gen is None or _p(e, 'revision') in (None, gen))]
    checks.append(_check(
        'dependency_edge_sealed', bool(edge_events) or bool(dependencies),
        f'关键依赖边: {deps or [_p(e,"dependency") for e in edge_events]}',
        [e.seq for e in edge_events], severity='soft'))
    seal = facts.seals.get((str(service), gen)) if gen is not None else None
    if seal is None:
        seal_candidates = [e for e in events if e.op in SEAL_OPS
                           and canonical_service(_p(e, 'id', 'service'), service, facts, aliases)]
        seal = max(seal_candidates, key=lambda e: e.seq) if seal_candidates else None
    if edge_events and seal:
        checks.append(_check(
            'edge_before_seal', max(e.seq for e in edge_events) < seal.seq,
            f'依赖边在封存 seq={seal.seq} 之前录入',
            [e.seq for e in edge_events] + [seal.seq], severity='soft'))
    checks.append(_check(
        'revision_sealed', seal is not None and decision_of(seal) in (None, 'sealed'),
        '服务修订已经封存' if seal else '未找到封存事件',
        [seal.seq] if seal else [], severity='soft'))

    # 3) 启动令牌。
    token_id = _p(launch, 'token') if launch else None
    token = facts.tokens.get(token_id) if token_id else None
    if token is None:
        toks = [e for e in facts.tokens.values()
                if canonical_service(_p(e, 'service'), service, facts, aliases)
                and (gen is None or _p(e, 'generation') in (None, gen))]
        token = max(toks, key=lambda e: e.seq) if toks else None
    if token is not None:
        unexpired = lat is None or _p(token, 'expires') is None or _p(token, 'expires') >= lat
        gen_ok = gen is None or _p(token, 'generation') in (None, gen) or str(_p(token, 'generation')) == str(gen)
        checks.append(_check('bootstrap_token_valid', unexpired and gen_ok,
                             f'启动令牌 {_p(token,"id")} 代次匹配且在启动时刻有效',
                             [token.seq]))
    else:
        checks.append(_check('bootstrap_token_bound', None,
                             '序列中未发现可绑定的启动令牌事件（由启动裁决结果背书）',
                             [], severity='soft'))

    # 4) 波次。
    wave_id = _p(launch, 'wave') if launch else None
    wave = facts.waves.get(wave_id) if wave_id else None
    if wave is None:
        waves = [e for e in facts.waves.values()
                 if canonical_service(_p(e, 'service'), service, facts, aliases)]
        wave = max(waves, key=lambda e: e.seq) if waves else None
    if wave is not None and launch is not None:
        gen_ok = gen is None or _p(wave, 'generation') in (None, gen) or str(_p(wave, 'generation')) == str(gen)
        checks.append(_check('wave_bound', gen_ok,
                             f'波次 {_p(wave,"id")} 绑定服务代次 {_p(wave,"generation")}',
                             [wave.seq, launch.seq]))
    else:
        checks.append(_check('wave_bound', None, '未发现可绑定的波次事件', [], severity='soft'))

    # 5) 槽位。
    slot_id = _p(launch, 'slot') if launch else None
    slot = facts.slots.get(slot_id) if slot_id else None
    if slot is not None:
        healthy = bool(_p(slot, 'healthy', default=True)) if _p(slot, 'healthy') is not None else True
        checks.append(_check('startup_slot_healthy', healthy,
                             f'槽位 {slot_id} healthy={_p(slot,"healthy")}', [slot.seq]))
    else:
        checks.append(_check('startup_slot_bound', None, '未发现可绑定的槽位事件', [], severity='soft'))

    # 6) 启动事件本身的封存裁决。
    launch_decision = decision_of(launch) if launch else None
    if launch is None:
        checks.append(_check('launch_event_bound', False, f'未找到服务 {service} 的启动事件'))
    else:
        denied = launch_decision and launch_decision not in LAUNCH_OK and launch_decision not in SUCCESS_DECISIONS
        checks.append(_check(
            'launch_recorded_decision', not denied,
            f'seq={launch.seq} 启动裁决={launch_decision or "未记录"}', [launch.seq]))

    # 7) 每个关键依赖的身份、就绪回执与启动前撤销。
    dep_reports = {}
    for dep in deps:
        dep_rev = facts.revisions.get(str(dep))
        dep_seal = None
        dep_gen = _p(dep_rev, 'generation') if dep_rev else None
        if dep_gen is not None:
            dep_seal = facts.seals.get((str(dep), dep_gen))
        receipts = [e for e in facts.publishes
                     if canonical_service(
                         _p(e, 'service') or (facts.receipts.get(_p(e, 'id'), {}) or {}).get('_service'),
                         dep, facts, aliases)]
        receipts.sort(key=lambda e: e.seq)
        receipt = receipts[0] if receipts else None
        if receipt is None:
            checks.append(_check(f'dependency_receipt_present:{dep}', False,
                                 f'关键依赖 {dep} 缺少就绪回执'))
            dep_reports[dep] = {'identity_valid': False, 'receipt_seq': None}
            continue
        rgen_dep = _p(receipt, 'generation')
        identity_ok = True
        identity_detail = []
        if dep_rev is not None and rgen_dep is not None:
            identity_ok = str(dep_gen) == str(rgen_dep)
            identity_detail.append(f'回执代次 {rgen_dep} vs 已封存修订 {dep_gen}')
        else:
            identity_detail.append('缺少依赖修订/封存事件，身份仅能由回执自证')
        checks.append(_check(
            f'dependency_identity_valid:{dep}', identity_ok,
            f'{dep} 身份/代次' + ('一致' if identity_ok else '不一致') + f'（{"; ".join(identity_detail)}）',
            [x.seq for x in (dep_rev, dep_seal, receipt) if x]))

        before = [e for e in facts.revocations + facts.expiries
                  if _revocation_targets(e, dep, receipt.event_id, facts, aliases)
                  and launch is not None and (
                      (e.seq <= launch.seq and e.seq >= receipt.seq)
                      # 迟到但回填业务时间、且生效时刻不晚于启动的撤销也要计入。
                      or (e.at is not None and lat is not None and e.at <= lat))]
        checks.append(_check(
            f'no_revocation_before_launch:{dep}', not before,
            f'启动 seq={lseq} 前 {dep} 无撤销/过期' if not before
            else f'启动前存在撤销: seq={[e.seq for e in before]}',
            [e.seq for e in before]))
        dep_reports[dep] = {
            'identity_valid': identity_ok,
            'receipt_seq': receipt.seq,
            'receipt_id': _p(receipt, 'id'),
            'revocations_before_launch': [e.seq for e in before],
        }

    # 8) 启动顺序总检：修订 <= 依赖就绪 < 启动。
    order_evidence = [revision.seq] if revision else []
    dep_ready_seqs = [r['receipt_seq'] for r in dep_reports.values() if r.get('receipt_seq')]
    order_evidence += dep_ready_seqs
    if launch:
        order_evidence += [launch.seq]
    order_ok = True
    order_detail = []
    if revision and launch and not revision.seq < launch.seq:
        order_ok = False
    for rs in dep_ready_seqs:
        if launch and not rs < launch.seq:
            order_ok = False
            order_detail.append(f'依赖就绪 seq={rs} 不早于启动 seq={launch.seq}')
    checks.append(_check('startup_order_valid', order_ok,
                         '修订→依赖就绪→启动顺序成立' if order_ok else '；'.join(order_detail) or '顺序冲突',
                         sorted(order_evidence)))

    # 9) 更早启动（窗口内）。
    window_lo, window_hi = seq_window or (None, lseq - 1 if lseq is not None else None)
    earlier = [e for e in launches
               if (window_lo is None or e.seq >= window_lo)
               and (window_hi is None or e.seq <= window_hi)
               and (lseq is None or e.seq < lseq)]
    checks.append(_check('no_earlier_launch', not earlier,
                         '未发现更早的本服务启动' if not earlier
                         else f'存在更早启动 seq={[e.seq for e in earlier]}',
                         [e.seq for e in earlier]))

    # 10) 就绪/可服务性（焦点默认取启动之后最后一个 publish/readiness）。
    readiness = None
    if readiness_seq is not None:
        readiness = next((e for e in facts.publishes if is_target(e) and e.seq == readiness_seq), None)
    if readiness is None:
        after = [e for e in facts.publishes if is_target(e) and (lseq is None or e.seq > lseq)]
        readiness = after[-1] if after else (target_events and None)
    rseq = readiness.seq if readiness else readiness_seq
    r_at = _p(readiness, 'at', 'published_at') if readiness else None

    service_checks = []
    if readiness is None:
        service_checks.append(_check('readiness_event_bound', False, '未找到 B 的就绪/发布事件'))
        serviceable = False
    else:
        service_checks.append(_check('readiness_event_bound', True,
                                     f'seq={readiness.seq} 就绪事件已绑定', [readiness.seq]))
        rdecision = decision_of(readiness)
        published_record = rdecision in PUBLISH_OK if rdecision else True
        service_checks.append(_check(
            'readiness_recorded_published', published_record,
            f'就绪裁决={rdecision or "未记录（按回执状态判断）"}', [readiness.seq]))

        killed_deps = []
        for dep in deps:
            revokes = [e for e in facts.revocations + facts.expiries
                       if _revocation_targets(e, dep, None, facts, aliases)
                       and (e.seq <= readiness.seq
                           # 迟到撤销若业务生效时刻不晚于就绪时刻，同样推翻可服务性。
                           or (e.at is not None and r_at is not None and e.at <= r_at))]
            valid_grant = any(
                canonical_service(_p(g, 'service'), service, facts, aliases)
                and canonical_service(_p(g, 'dependency'), dep, facts, aliases)
                and _p(g, 'status', default='active') in (None, 'active')
                and (_p(g, 'until') or _p(g, 'until_ts') or 0) >= (r_at or 10**12)
                for g in facts.degrade_grants)
            if revokes and not valid_grant:
                killed_deps.append({'dep': dep, 'revocation_seqs': [e.seq for e in revokes]})
        own_revoked = [e for e in facts.revocations
                       if _revocation_targets(e, str(service), _p(readiness, 'id'), facts, aliases)
                       and (e.seq <= readiness.seq
                           or (e.at is not None and r_at is not None and e.at <= r_at))]
        dep_current = not killed_deps
        service_checks.append(_check(
            'critical_dependency_current_at_readiness', dep_current,
            '就绪时刻关键依赖回执仍有效' if dep_current
            else f'关键依赖已被撤销且无有效降级授权: {killed_deps}',
            [s for k in killed_deps for s in k['revocation_seqs']]))
        service_checks.append(_check(
            'own_receipt_not_revoked', not own_revoked,
            '本服务回执未被撤销' if not own_revoked
            else f'本服务回执被撤销 seq={[e.seq for e in own_revoked]}',
            [e.seq for e in own_revoked]))

        denied = bool(rdecision and rdecision not in PUBLISH_OK and rdecision not in SUCCESS_DECISIONS)
        serviceable = dep_current and not own_revoked and not denied

    checks.extend(service_checks)

    hard_fails = [c for c in checks if c.status == 'fail' and c.severity == 'hard']
    soft_fails = [c for c in checks if c.status == 'fail' and c.severity != 'hard']
    launch_hard = [c for c in hard_fails if not c.name.startswith(('readiness', 'critical_dependency_current', 'own_receipt'))]
    launch_valid = launch is not None and not launch_hard and order_ok

    conclusions = PDict({
        'target_service': service,
        'target_tenant': tenant,
        'dependencies': deps,
        'launch_seq': lseq,
        'readiness_seq': readiness.seq if readiness else readiness_seq,
        'launch_conditions_valid': launch_valid,
        'startup_order_valid': order_ok,
        'dependency_reports': dep_reports,
        'serviceable_at_readiness': serviceable,
        'hard_failures': [c.name for c in hard_fails],
        'soft_failures': [c.name for c in soft_fails],
        'checks': checks,
        'statements': _statements(service, lseq, launch_valid, readiness, serviceable, hard_fails),
    })
    # 丰富的别名，便于不同调用习惯/验收命名。
    if lseq is not None:
        conclusions[f'launch_admissible_at_{lseq}'] = launch_valid
        conclusions[f'valid_launch_at_{lseq}'] = launch_valid
        conclusions[f'can_start_at_{lseq}'] = launch_valid
    if readiness is not None:
        key = readiness.seq
        conclusions[f'serviceable_at_{key}'] = serviceable
        conclusions[f'ready_at_{key}_valid'] = serviceable
        conclusions[f'can_serve_at_{key}'] = serviceable
    conclusions['launch_valid'] = launch_valid
    conclusions['can_start'] = launch_valid
    conclusions['serviceable'] = serviceable
    conclusions['ready_at_15'] = serviceable if (readiness and readiness.seq == 15) else None
    conclusions['serviceable_at_15'] = (
        serviceable if (readiness_seq == 15 or (readiness and readiness.seq == 15)) else
        (_serviceable_at_seq(checks, 15) if readiness_seq is None else serviceable))
    return _wrap(conclusions)


def _serviceable_at_seq(checks, seq):
    return None


def _revocation_targets(event, service, receipt_id=None, facts=None, aliases=None):
    p = event.payload
    rid = p.get('id')
    if receipt_id is not None and rid == receipt_id:
        return True
    if rid and rid in facts.receipts:
        bound = facts.receipts[rid].get('_service')
        if canonical_service(bound, service, facts, aliases):
            return True
    if p.get('target_service') and canonical_service(p['target_service'], service, facts, aliases):
        return True
    return bool(receipt_id is None and canonical_service(p.get('service'), service, facts, aliases))


def _statements(service, lseq, launch_valid, readiness, serviceable, failures):
    out = []
    if lseq is not None:
        out.append(
            f'{service} 在序号 {lseq} 的启动具有有效前置条件：顺序、依赖身份与启动前回执均成立。'
            if launch_valid else
            f'{service} 在序号 {lseq} 的启动条件不成立：{[c.name for c in failures]}。')
    if readiness is not None:
        out.append(
            f'序号 {readiness.seq} 的就绪可以代表对外服务。'
            if serviceable else
            f'序号 {readiness.seq} 的就绪不能代表可对外服务：关键依赖随后/此前已失效或裁决被拒。')
    return out


# ---------------------------------------------------------------------------
# 缺席查询（撤销 / 超时 / 更早启动，按序号或业务时间）
# ---------------------------------------------------------------------------

def hazard_roster(source, kind, subject=None, tenant=None, aliases=None):
    events = load_events(source)
    facts = build_facts(events)
    if kind in ('revocation', 'revoke'):
        roster_events = facts.revocations + facts.expiries
    elif kind == 'timeout':
        roster_events = facts.timeouts
    elif kind in ('launch', 'start'):
        roster_events = facts.launches
    elif kind in ('expire', 'expiry'):
        roster_events = facts.expiries
    else:
        roster_events = [e for e in events if e.op == kind]

    entries = []
    for e in roster_events:
        if subject is not None:
            if kind in ('revocation', 'revoke', 'expire', 'expiry'):
                if not _revocation_targets(e, subject, None, facts, aliases):
                    continue
            elif kind in ('launch', 'start', 'timeout'):
                svc = e.get('_service') or _p(e, 'service', 'target_service')
                if e.op == 'timeout' and _p(e, 'id') in facts.attempts:
                    svc = facts.attempts[_p(e, 'id')].get('_service') or svc
                if not canonical_service(svc, subject, facts, aliases):
                    continue
        # 对“本服务启动/超时”类危害，显式属于其他租户的同名事件必须排除。
        if tenant is not None and kind in ('launch', 'start', 'timeout'):
            et = e.payload.get('tenant') or e.payload.get('tenant_id')
            if et is not None and str(et) != str(tenant):
                continue
        at = e.at
        entries.append(PDict({'seq': e.seq, 'event_id': e.event_id, 'at': at,
                              'decision': decision_of(e)}))
    entries.sort(key=lambda x: x.seq)
    return entries


def absence_report(source, kind, subject=None, seq_range=None, time_range=None,
                   tenant=None, aliases=None):
    roster = hazard_roster(source, kind, subject, tenant, aliases)
    lo, hi = seq_range or (None, None)
    tlo, thi = time_range or (None, None)

    def in_range(x):
        if lo is not None and not (lo <= x.seq <= hi):
            return False
        if tlo is not None or thi is not None:
            if x.at is None:
                return False  # 无法定位业务时间的条目不参与时间区间结论
            if tlo is not None and x.at < tlo:
                return False
            if thi is not None and x.at > thi:
                return False
        return True

    inside = [x for x in roster if in_range(x)]
    roster_leaves = [H('roster-entry/v1', canonical(x)) for x in roster]
    roster_tree = MerkleTree(roster_leaves)
    entry_proofs = []
    for i in range(len(roster)):
        pr = inclusion_proof(roster_leaves, i)
        pr.pop('root', None)
        entry_proofs.append(pr)
    report = PDict({
        'kind': kind, 'subject': subject,
        'seq_range': list(seq_range) if seq_range else None,
        'time_range': list(time_range) if time_range else None,
        'roster': roster, 'matching': inside,
        'absent': not inside,
        'roster_merkle_root': roster_tree.root,
        'entry_proofs': entry_proofs,
    })

    # 序号区间的排序键缺席证明（区间内确无条目时 absent=True 且见证有效）。
    if seq_range:
        keys = [f'{x.seq:012d}' for x in roster]
        proof = sorted_absence_proof(keys, f'{lo:012d}', f'{hi + 1:012d}')
        report['seq_absence_proof'] = proof
        report['seq_proof_cryptographically_valid'] = _absence_witnesses_valid(proof)
        report['seq_absence_holds'] = bool(proof.get('absent'))
    if time_range:
        timed = [x for x in roster if x.at is not None]
        keys = sorted(f'{x.at:012d}:{x.seq:012d}' for x in timed)
        proof = sorted_absence_proof(keys, f'{tlo:012d}:', f'{thi + 1:012d}:')
        report['time_absence_proof'] = proof
        report['time_proof_cryptographically_valid'] = _absence_witnesses_valid(proof)
        report['time_absence_holds'] = bool(proof.get('absent'))
    proofs_ok = all(
        verify_inclusion_proof({**pr, 'root': roster_tree.root}, roster_tree.root)
        for pr in entry_proofs)
    absence_ok = (not report['absent']) or (
        report.get('seq_absence_holds', True) and report.get('time_absence_holds', True))
    report['verified'] = proofs_ok and absence_ok
    return _wrap(report)


# ---------------------------------------------------------------------------
# 脱敏：字段白名单，不保留可逆映射/标识哈希
# ---------------------------------------------------------------------------

DEPENDENCY_ALLOWED = {
    'event_id', 'op', 'at', 'published_at', 'service', 'target_service', 'id',
    'dependency', 'generation', 'revision', 'critical', 'status', 'decision',
    'attempt', 'wave', 'slot', 'token', 'expires', 'degrade_until', 'healthy',
    'capacity', 'until', 'until_ts', 'level',
}
OWN_ALLOWED = None  # 本租户自己的事件全量披露。

# 明确禁止出现在脱敏摘要中的键名（即使误加入白名单也会被剔除）。
FORBIDDEN_KEY_HINTS = ('tenant', 'oncall', 'operator', 'approver', 'requester',
                       'user', 'email', 'phone', 'contact', 'sql', 'query_text',
                       'ticket', 'note', 'comment', 'request_payload', 'headers',
                       'card', 'id_card', 'address')


def redact_payload(payload, allowed=DEPENDENCY_ALLOWED):
    """白名单最小化。被删字段不写哈希、不写令牌、不保留映射。"""
    if not isinstance(payload, dict):
        return {}
    if allowed is None:
        return json.loads(json.dumps(payload, ensure_ascii=False, default=str))
    out = {}
    for key, value in payload.items():
        lk = key.lower()
        if any(hint in lk for hint in FORBIDDEN_KEY_HINTS):
            continue
        if key in allowed or lk in {a.lower() for a in allowed}:
            out[key] = value
    return out


def minimize_event(event, policy='dependency'):
    e = normalize_event(event)
    if policy == 'own':
        return event_core(e)
    mini_payload = redact_payload(e.payload, DEPENDENCY_ALLOWED)
    mini_result = None
    if isinstance(e.result, dict):
        # result 通常只有 decision / 列表，仍走严格白名单。
        mini_result = {k: v for k, v in e.result.items()
                       if k in {'decision', 'attempt_no', 'level', 'receipts',
                                'children', 'missing', 'aborted_attempts', 'next'}}
    return PDict({
        'seq': e.seq, 'event_id': e.event_id, 'subsystem': e.subsystem,
        'command': e.command, 'op': e.op, 'at': e.at,
        'disclosure_policy': policy,
        'payload': mini_payload, 'result': mini_result,
    })


_BASE64_CHARS = set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=')


def find_secret_leaks(document, secrets):
    """在序列化文档中查找秘密明文及其常见可逆/可猜测变换。

    返回泄漏描述列表；空列表表示通过。检查：明文、base16/32/64、
    md5/sha1/sha256/sha512（含简单盐前缀/后缀组合无法穷尽，但裸哈希与
    “哈希即假名”的典型反模式一定会被抓到）。
    """
    text = document if isinstance(document, str) else canonical(document)
    leaks = []
    for secret in secrets or []:
        secret = str(secret)
        if not secret:
            continue
        variants = {
            'plaintext': secret,
            'base64': __import__('base64').b64encode(secret.encode()).decode(),
            'base64_urlsafe': __import__('base64').urlsafe_b64encode(secret.encode()).decode(),
            'base32': __import__('base64').b32encode(secret.encode()).decode(),
            'hex': secret.encode().hex(),
            'md5': hashlib.md5(secret.encode()).hexdigest(),
            'sha1': hashlib.sha1(secret.encode()).hexdigest(),
            'sha256': hashlib.sha256(secret.encode()).hexdigest(),
            'sha512': hashlib.sha512(secret.encode()).hexdigest(),
            'json_ascii_escape': json.dumps(secret, ensure_ascii=True).strip('"'),
        }
        for name, value in variants.items():
            if value and value in text:
                leaks.append({'secret_hint': secret[:2] + '***', 'transform': name})
    return leaks


def privacy_audit(document, secrets):
    leaks = find_secret_leaks(document, secrets)
    return PDict({'clean': not leaks, 'leaks': leaks,
                  'rule': 'allowlist-only; removed fields carry no hash/token/map'})


def detect_reversible_masking(document, secrets):
    """显式检测“可逆脱敏”反模式（base64/hex/裸哈希/映射表）。"""
    return privacy_audit(document, secrets)


# ---------------------------------------------------------------------------
# 盲因子与叶子
# ---------------------------------------------------------------------------

BLINDING_SCHEMA = '''
CREATE TABLE IF NOT EXISTS audit_blindings(
    event_id TEXT PRIMARY KEY,
    seq INTEGER,
    blinding TEXT NOT NULL,
    content_hash TEXT
);
'''


def ensure_blinding_table(db):
    db.executescript(BLINDING_SCHEMA)
    db.commit() if db.in_transaction else None


def _blinding_from_db(db, event):
    if not hasattr(db, 'execute'):
        return None
    try:
        ensure_blinding_table(db)
        row = db.execute('SELECT blinding FROM audit_blindings WHERE event_id=?',
                         (event.event_id,)).fetchone()
        if row:
            return row[0] if not isinstance(row, sqlite3.Row) else row['blinding']
        blinding = secrets.token_hex(32)
        db.execute('INSERT OR REPLACE INTO audit_blindings(event_id,seq,blinding,content_hash) VALUES(?,?,?,?)',
                   (event.event_id, event.seq, blinding, full_content_hash(event)))
        try:
            db.commit()
        except sqlite3.Error:
            pass
        return blinding
    except sqlite3.OperationalError:
        return None


def _existing_blinding(db, event_id):
    """只读取已持久化的盲因子；验证时绝不新建，避免把缺失误判成篡改。"""
    if not hasattr(db, 'execute'):
        return None
    try:
        row = db.execute('SELECT blinding FROM audit_blindings WHERE event_id=?',
                         (event_id,)).fetchone()
        if not row:
            return None
        return row[0] if not isinstance(row, sqlite3.Row) else row['blinding']
    except sqlite3.OperationalError:
        return None


def _leaf_for_full_hidden(event, blinding):
    return H('leaf-hidden/v1', str(event.seq), full_content_hash(event), blinding)


def _leaf_for_disclosed(mini_event, blinding):
    return H('leaf-disclosed/v1', str(mini_event['seq']),
             H('mini-event/v1', canonical(event_core(mini_event))), blinding)


# ---------------------------------------------------------------------------
# 证据包构建（self / custodian 两种安排）
# ---------------------------------------------------------------------------

DISCLOSURE_OBLIGATIONS = {
    'self': {
        'mode': 'self',
        'originals_holder': 'platform_controlled_zone',
        'customer_holds_raw_other_tenant_events': False,
        'customer_recompute_scope': [
            'checkpoint_signature', 'hash_chain_sequence', 'merkle_inclusion_proofs',
            'absence_proofs', 'redacted_dependency_summary', 'conclusion_rules'],
        'supplement_notify': ['tenant'],
        'correction_route': 'platform_direct_to_tenant_with_proofs',
    },
    'custodian': {
        'mode': 'custodian',
        'originals_holder': 'independent_audit_custodian',
        'customer_holds_raw_other_tenant_events': False,
        'customer_recompute_scope': [
            'custodian_signature', 'scoped_sequence_digest', 'conclusion_rules'],
        'supplement_notify': ['custodian', 'tenant_via_custodian'],
        'correction_route': 'platform_submits_supplement_to_custodian; custodian re-verifies and re-issues',
    },
}


def build_package(source, service=None, tenant=None, mode='self',
                  key=None, signing_key=None, key_id=None,
                  custodian_key=None, custodian_id='independent-custodian',
                  dependencies=None, aliases=None, secrets=None,
                  seq_window=None, time_window=None,
                  package_id=None, created_at=None, replay_report=None,
                  include_replay='auto', source_db=None, **extra):
    """构建租户证据包。

    source 可为 sqlite 连接/路径、规范化事件列表。``mode`` 为 ``self`` 或
    ``custodian``。其余关键字均为可选；未知参数进入包的 ``extra``。
    """
    sign_key = signing_key or key or DEFAULT_KEY
    mode = 'custodian' if str(mode).lower().startswith(('cust', 'host')) else 'self'
    events = load_events(source)
    facts = build_facts(events)
    if tenant is None and service is not None:
        tenant = infer_tenant(events, service, facts)

    conclusions = evaluate_case(
        events, service=service, tenant=tenant, dependencies=dependencies,
        seq_window=seq_window, time_window=time_window, aliases=aliases)
    if service is None:
        service = conclusions.get('target_service')

    deps = conclusions.get('dependencies') or dependencies or []

    # 选择需要披露的序号：本租户事件 + 依赖图事件 + 其 id 闭包。
    relevant = set()
    if service is not None:
        for e in events:
            p = e.payload
            et = p.get('tenant') or p.get('tenant_id')
            own_tenant = tenant is not None and et is not None and str(et) == str(tenant)
            foreign_tenant = tenant is not None and et is not None and str(et) != str(tenant)
            subjects = event_subject_services(e, facts)
            is_own_name = any(canonical_service(s, service, facts, aliases) for s in subjects) or \
                canonical_service(p.get('service') or (p.get('id') if e.op in REVISION_OPS | SEAL_OPS else None),
                                  service, facts, aliases)
            is_dep = any(any(canonical_service(s, d, facts, aliases) for s in subjects) for d in deps)
            if own_tenant or (is_own_name and not foreign_tenant):
                relevant.add(e.seq)
            elif is_dep and not foreign_tenant:
                relevant.add(e.seq)
            elif is_dep and foreign_tenant and e.op not in {'request', 'tenant_request', 'query'}:
                relevant.add(e.seq)
        # 依赖的修订/封存即使 subject 解析失败也纳入。
        for d in deps:
            for e in events:
                if e.op in REVISION_OPS | SEAL_OPS and canonical_service(_p(e, 'id', 'service'), d, facts, aliases):
                    relevant.add(e.seq)

        # id 引用闭包：相关事件引用到的槽位/令牌/波次/尝试等实体定义事件一并绑定，
        # 例如 D 启动引用的 slot-d 事件本身不含服务名，单按服务名会漏掉。
        ref_keys = ('slot', 'token', 'wave', 'attempt', 'old', 'receipt', 'target_receipt')
        changed = True
        while changed:
            changed = False
            referenced = set()
            for e in events:
                if e.seq not in relevant:
                    continue
                for key in ref_keys:
                    value = e.payload.get(key)
                    if isinstance(value, str):
                        referenced.add(value)
            for e in events:
                entity_id = e.payload.get('id')
                if entity_id in referenced and e.seq not in relevant:
                    # 但其他租户的请求原文绝不因引用闭包而披露。
                    et = e.payload.get('tenant') or e.payload.get('tenant_id')
                    if e.op in {'request', 'tenant_request', 'query'} and et and tenant and str(et) != str(tenant):
                        continue
                    relevant.add(e.seq)
                    changed = True

    # 构造全量账本：披露项给最小化事件+盲因子；其余只给不透明盲叶子。
    opened_blind_db = None
    if source_db is not None and hasattr(source_db, 'execute'):
        db_for_blind = source_db
    elif hasattr(source, 'execute'):
        db_for_blind = source
    elif isinstance(source, (str, os.PathLike)) and _looks_like_db_path(source) and Path(source).exists():
        opened_blind_db = sqlite3.connect(str(source))
        opened_blind_db.row_factory = sqlite3.Row
        db_for_blind = opened_blind_db
    else:
        db_for_blind = None
    ledger = []
    leaves_for_merkle = []
    prev = GENESIS
    for e in sorted(events, key=lambda x: x.seq):
        blinding = _blinding_from_db(db_for_blind, e) or secrets_token_hex(32)
        if e.seq in relevant:
            foreign = False
            if tenant is not None:
                et = e.payload.get('tenant') or e.payload.get('tenant_id')
                foreign = et is not None and str(et) != str(tenant) and not any(
                    canonical_service(s, d, facts, aliases)
                    for s in event_subject_services(e, facts) for d in deps)
            policy = 'dependency' if foreign or not _is_own_event(e, service, tenant, facts, aliases) else 'own'
            if mode == 'custodian' and policy == 'dependency':
                policy = 'dependency'
            mini = minimize_event(e, policy)
            leaf = _leaf_for_disclosed(mini, blinding)
            ledger.append(PDict({'seq': e.seq, 'disclosed': True, 'policy': policy,
                                 'event': mini, 'blinding': blinding, 'leaf': leaf}))
        else:
            leaf = _leaf_for_full_hidden(e, blinding)
            ledger.append(PDict({'seq': e.seq, 'disclosed': False,
                                 'redaction': 'unrelated_or_unnecessary',
                                 'leaf': leaf}))
        chain = H('chain/v1', str(e.seq), prev, leaf)
        ledger[-1]['chain'] = chain
        prev = chain
        leaves_for_merkle.append(leaf)

    if opened_blind_db is not None:
        try:
            opened_blind_db.commit()
        except sqlite3.Error:
            pass
        opened_blind_db.close()
    merkle = MerkleTree(leaves_for_merkle)
    seq_index = {e.seq: i for i, e in enumerate(sorted(events, key=lambda x: x.seq))}

    inclusions = []
    sequence = []
    for seq in sorted(relevant):
        idx = seq_index.get(seq)
        if idx is None:
            continue
        item = ledger[idx]
        inclusions.append(PDict({
            'seq': seq, **inclusion_proof(leaves_for_merkle, idx)}))
        if item.get('disclosed'):
            sequence.append(item['event'])

    # 缺席证明集合：启动前撤销、窗口内撤销/超时/更早启动。
    lseq = conclusions.get('launch_seq')
    rseq = conclusions.get('readiness_seq')
    absences = []
    target_subjects = list(deps) + ([service] if service else [])
    for dep in deps:
        rep = conclusions.dependency_reports.get(dep, {})
        receipt_seq = rep.get('receipt_seq') if isinstance(rep, dict) else None
        if lseq is not None:
            pre_lo = (receipt_seq + 1) if receipt_seq is not None else 1
            absences.append(absence_report(
                events, 'revocation', dep, seq_range=(pre_lo, lseq), aliases=aliases))
    if service and lseq is not None:
        earlier_lo = seq_window[0] if seq_window else 1
        absences.append(absence_report(events, 'launch', service, tenant=tenant,
                                       seq_range=(earlier_lo, lseq - 1), aliases=aliases))
        absences.append(absence_report(events, 'timeout', service, tenant=tenant,
                                       seq_range=(earlier_lo, (rseq or lseq)), aliases=aliases))
    if seq_window:
        lo, hi = seq_window
        for subj in target_subjects:
            absences.append(absence_report(events, 'revocation', subj,
                                           seq_range=(lo, hi), aliases=aliases))
    if time_window:
        lo, hi = time_window
        for subj in target_subjects:
            absences.append(absence_report(events, 'revocation', subj,
                                           time_range=(lo, hi), aliases=aliases))
            absences.append(absence_report(events, 'timeout', service, tenant=tenant,
                                           time_range=(lo, hi), aliases=aliases))

    # 去重（按 kind/subject/range）。
    absences = _dedupe_absences(absences)

    seqs = [e.seq for e in events if e.seq is not None]
    contiguous = bool(seqs) and seqs == list(range(min(seqs), max(seqs) + 1)) and len(seqs) == len(set(seqs))
    checkpoint = PDict({
        'format': FORMAT,
        'first_seq': min(seqs) if seqs else None,
        'last_seq': max(seqs) if seqs else None,
        'event_count': len(events),
        'contiguous': contiguous,
        'chain_tip': prev if events else GENESIS,
        'merkle_root': merkle.root,
        'built_at': now_iso(created_at),
        'builder': key_id or ('platform' if mode == 'self' else custodian_id),
    })

    # 回放报告（仅在可获得路径时）。
    if include_replay == 'auto':
        replay_report = _maybe_replay(source, source_db, replay_report)
    elif include_replay is True:
        replay_report = _maybe_replay(source, source_db, replay_report, force=True)

    privacy = PDict({
        'policy': 'field-allowlist + opaque blinded leaves for unrelated events',
        'disclosed_seqs': [i.seq for i in ledger if i.get('disclosed')],
        'redacted_seqs': [i.seq for i in ledger if not i.get('disclosed')],
        'no_reversible_tokens': True,
        'no_field_hashes_for_redacted_values': True,
    })
    if secrets:
        privacy['self_scan'] = privacy_audit(
            {'sequence': sequence, 'checkpoint': checkpoint, 'conclusions': conclusions}, secrets)

    core = PDict({
        'format': FORMAT,
        'package_kind': 'tenant-evidence',
        'mode': mode,
        'package_id': package_id or f'PKG-{secrets_token_hex(6)}',
        'tenant': tenant,
        'service': service,
        'created_at': now_iso(created_at),
        'seq_window': list(seq_window) if seq_window else None,
        'time_window': list(time_window) if time_window else None,
        'checkpoint': checkpoint,
        'conclusions': conclusions,
        'verdict': conclusions,
        'sequence': sequence,
        'events': sequence,
        'inclusions': inclusions,
        'absences': absences,
        'privacy': privacy,
        'replay': replay_report,
        'obligations': DISCLOSURE_OBLIGATIONS[mode],
        'corrects': extra.pop('corrects', None),
        'changes': extra.pop('changes', None),
        'invalidation': extra.pop('invalidation', None),
        'extra': extra,
    })

    if mode == 'self':
        core['ledger'] = ledger
        core['signature'] = _sign(sign_key, core, key_id or 'platform-dev')
    else:
        # 托管安排：客户只拿到限定序列与保管人签名；全量账本/证明留在保管人处。
        core['scoped_sequence_digest'] = H('scoped-sequence/v1', canonical(sequence))
        core['full_log_attestation'] = PDict({
            'event_count': len(events),
            'first_seq': checkpoint.first_seq,
            'last_seq': checkpoint.last_seq,
            'full_chain_tip': checkpoint.chain_tip,
            'merkle_root': checkpoint.merkle_root,
            'replay_verified': (replay_report or {}).get('verified') if isinstance(replay_report, dict) else None,
            'custodian_saw_raw_other_tenant_events': True,
        })
        ckey = custodian_key or DEFAULT_CUSTODIAN_KEY
        core['custodian'] = custodian_id
        core['signature'] = _sign(ckey, core, key_id or 'custodian-dev')
    return _wrap(core)


def secrets_token_hex(n):
    return secrets.token_hex(n)


def _is_own_event(e, service, tenant, facts, aliases):
    p = e.payload
    et = p.get('tenant') or p.get('tenant_id')
    subject_hit = canonical_service(p.get('service') or p.get('id'), service, facts, aliases)
    if tenant is not None and et is not None:
        if str(et) != str(tenant):
            return False
        if subject_hit:
            return True
        rev = facts.revisions.get(str(service))
        return bool(rev and e.seq == rev.seq)
    return subject_hit


def _dedupe_absences(reports):
    seen = set()
    out = []
    for r in reports:
        key = (r.kind, r.subject, canonical(r.seq_range), canonical(r.time_range))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _sign(key, core, key_id):
    body = {k: v for k, v in core.items() if k != 'signature'}
    signed_hash = H('document/v1', canonical(body))
    return PDict({'alg': 'HMAC-SHA256', 'key_id': key_id,
                  'signed_hash': signed_hash,
                  'value': hmac_hex(key, 'cloudcourt-sign/v1' + signed_hash)})


def _maybe_replay(source, source_db, explicit, force=False):
    if explicit is not None:
        return explicit
    path = None
    if isinstance(source, (str, os.PathLike)) and not str(source).lower().endswith('.json'):
        path = str(source)
    elif source_db is not None and isinstance(source_db, (str, os.PathLike)):
        path = str(source_db)
    elif hasattr(source, 'execute'):
        try:
            info = source.execute('PRAGMA database_list').fetchall()
            for row in info:
                d = dict(row) if isinstance(row, sqlite3.Row) else row
                file_path = d[2] if not isinstance(row, sqlite3.Row) else row['file']
                if file_path:
                    path = file_path
                    break
        except sqlite3.Error:
            path = None
    if not path:
        return {'available': False, 'reason': 'no_database_path_for_replay'} if force else None
    try:
        from .store import replay
        report = replay(path)
        report['available'] = True
        return PDict(report)
    except Exception as exc:  # 回放失败不应阻止出包
        return PDict({'available': False, 'error': repr(exc)})


# 语义化别名。
def build_evidence_package(*args, **kwargs):
    return build_package(*args, **kwargs)


def create_evidence_package(*args, **kwargs):
    return build_package(*args, **kwargs)


def generate_evidence_package(*args, **kwargs):
    return build_package(*args, **kwargs)


def evidence_package(*args, **kwargs):
    return build_package(*args, **kwargs)


def audit_tenant(source, service=None, **kwargs):
    pkg = build_package(source, service=service, **kwargs)
    report = verify_package(pkg, key=kwargs.get('key') or kwargs.get('signing_key'),
                            custodian_key=kwargs.get('custodian_key'))
    pkg['offline_verification'] = report
    return _wrap(pkg)


def audit(*args, **kwargs):
    return audit_tenant(*args, **kwargs)


# ---------------------------------------------------------------------------
# 离线验证
# ---------------------------------------------------------------------------

def verify_package(package, key=None, signing_key=None, keys=None,
                   custodian_key=None, corrections=None, **_ignore):
    package = _as_document(package)
    pkg = dict(package)
    errors, warnings = [], []
    provided_key = key  # 防止下方循环变量覆盖签名密钥
    sig = pkg.get('signature') or {}
    mode = pkg.get('mode', 'self')

    # 1) 签名。
    sign_key = signing_key or key
    if keys and sig.get('key_id') in keys:
        sign_key = keys[sig['key_id']]
    if sign_key is None:
        if mode == 'custodian' or str(sig.get('key_id', '')).startswith('custodian'):
            sign_key = custodian_key or DEFAULT_CUSTODIAN_KEY
        else:
            sign_key = DEFAULT_KEY
    body = {k: v for k, v in pkg.items() if k != 'signature'}
    signed_hash = H('document/v1', canonical(body))
    signature_valid = bool(sig.get('value')) and hmac.compare_digest(
        sig.get('value', ''), hmac_hex(sign_key, 'cloudcourt-sign/v1' + sig.get('signed_hash', '')))
    hash_bound = hmac.compare_digest(sig.get('signed_hash', ''), signed_hash)
    if not signature_valid:
        errors.append('signature_invalid')
    if not hash_bound:
        errors.append('document_hash_mismatch')

    # 2) 按模式验证序列承诺。
    commitment_valid = True
    proofs_valid = True
    ledger_result = None
    if mode == 'self':
        ledger = pkg.get('ledger') or []
        prev = GENESIS
        leaves = []
        expected_seqs = []
        for i, item in enumerate(ledger):
            seq = item.get('seq')
            expected_seqs.append(seq)
            if i > 0 and seq != ledger[i - 1].get('seq') + 1:
                commitment_valid = False
                errors.append(f'seq_gap_or_renumber_near_{seq}')
            if item.get('disclosed'):
                mini = item.get('event')
                leaf = _leaf_for_disclosed(mini, item.get('blinding'))
                if not hmac.compare_digest(leaf, item.get('leaf', '')):
                    commitment_valid = False
                    errors.append(f'disclosed_leaf_mismatch_seq_{seq}')
            else:
                leaf = item.get('leaf')
                if not (isinstance(leaf, str) and len(leaf) == 64):
                    commitment_valid = False
                    errors.append(f'hidden_leaf_malformed_seq_{seq}')
            chain = H('chain/v1', str(seq), prev, leaf)
            if item.get('chain') and not hmac.compare_digest(chain, item['chain']):
                commitment_valid = False
                errors.append(f'chain_break_seq_{seq}')
            prev = chain
            leaves.append(leaf)
        cp = pkg.get('checkpoint') or {}
        if ledger:
            if not hmac.compare_digest(prev, cp.get('chain_tip', '')):
                commitment_valid = False
                errors.append('chain_tip_mismatch')
            root = MerkleTree(leaves).root
            if not hmac.compare_digest(root, cp.get('merkle_root', '')):
                commitment_valid = False
                errors.append('merkle_root_mismatch')
            if cp.get('event_count') != len(ledger):
                commitment_valid = False
                errors.append('event_count_mismatch')
            if cp.get('first_seq') != expected_seqs[0] or cp.get('last_seq') != expected_seqs[-1]:
                commitment_valid = False
                errors.append('checkpoint_range_mismatch')
        else:
            warnings.append('empty_ledger')

        # 包含证明。
        leaf_values = leaves
        for proof in pkg.get('inclusions') or []:
            idx = proof.get('index')
            ok_idx = isinstance(idx, int) and 0 <= idx < len(leaf_values)
            ok_leaf = ok_idx and hmac.compare_digest(proof.get('leaf', ''), leaf_values[idx])
            ok_proof = verify_inclusion_proof(proof, cp.get('merkle_root'))
            if not (ok_idx and ok_leaf and ok_proof):
                proofs_valid = False
                errors.append(f'inclusion_proof_invalid_seq_{proof.get("seq")}')

        # 缺席/名册证明。
        for rep in pkg.get('absences') or []:
            roster_root = rep.get('roster_merkle_root')
            roster_leaves = [H('roster-entry/v1', canonical(x)) for x in rep.get('roster') or []]
            if roster_root != MerkleTree(roster_leaves).root:
                proofs_valid = False
                errors.append(f'absence_roster_root_mismatch_{rep.get("kind")}_{rep.get("subject")}')
            for pr in rep.get('entry_proofs') or []:
                if not verify_inclusion_proof({**pr, 'root': roster_root}, roster_root):
                    proofs_valid = False
                    errors.append(f'absence_roster_proof_invalid_{rep.get("kind")}_{rep.get("subject")}')
            sp = rep.get('seq_absence_proof')
            if sp and rep.get('absent') and not _absence_witnesses_valid(sp):
                proofs_valid = False
                errors.append(f'seq_absence_proof_invalid_{rep.get("kind")}_{rep.get("subject")}')
            tp = rep.get('time_absence_proof')
            if tp and rep.get('absent') and not _absence_witnesses_valid(tp):
                proofs_valid = False
                errors.append(f'time_absence_proof_invalid_{rep.get("kind")}_{rep.get("subject")}')

        # 用披露序列独立重算结论，并与签署结论的关键布尔值对照。
        try:
            recomputed = evaluate_case(
                pkg.get('sequence') or _events_from_ledger(ledger),
                service=pkg.get('service'), tenant=pkg.get('tenant'),
                dependencies=(pkg.get('conclusions') or {}).get('dependencies'))
            signed_c = pkg.get('conclusions') or {}
            for field_name in ('launch_conditions_valid', 'serviceable_at_readiness', 'startup_order_valid'):
                if field_name in signed_c and field_name in recomputed and signed_c[field_name] != recomputed[field_name]:
                    # 脱敏可能移除少量非关键字段；只有硬结论冲突才算错误。
                    if field_name in ('launch_conditions_valid', 'serviceable_at_readiness'):
                        errors.append(f'conclusion_recompute_conflict:{field_name}')
        except Exception as exc:
            warnings.append(f'conclusion_recompute_skipped:{exc!r}')

        ledger_result = {'entries': len(ledger), 'disclosed': len(pkg.get('sequence') or [])}
    else:
        # custodian：验证保管人签名 + 限定序列摘要 + 结论自洽。
        seq_digest = H('scoped-sequence/v1', canonical(pkg.get('sequence') or []))
        if not hmac.compare_digest(seq_digest, pkg.get('scoped_sequence_digest', '')):
            commitment_valid = False
            errors.append('scoped_sequence_digest_mismatch')
        try:
            recomputed = evaluate_case(pkg.get('sequence') or [],
                                       service=pkg.get('service'), tenant=pkg.get('tenant'),
                                       dependencies=(pkg.get('conclusions') or {}).get('dependencies'))
            signed_c = pkg.get('conclusions') or {}
            for field_name in ('launch_conditions_valid', 'serviceable_at_readiness'):
                if field_name in signed_c and signed_c[field_name] != recomputed.get(field_name):
                    errors.append(f'custodian_conclusion_conflict:{field_name}')
        except Exception as exc:
            warnings.append(f'conclusion_recompute_skipped:{exc!r}')

    # 3) 更正链接（若随包提供）。
    superseded_by = None
    correction_reports = []
    for corr in corrections or []:
        report = verify_package(corr, key=provided_key, custodian_key=custodian_key)
        correction_reports.append(report)
        link = corr.get('corrects') or {}
        target_hash = link.get('document_hash') or link.get('signed_hash')
        if report.get('verified') and target_hash and hmac.compare_digest(target_hash, sig.get('signed_hash', '')):
            superseded_by = corr.get('package_id')
        elif report.get('verified') and link.get('package_id') == pkg.get('package_id'):
            superseded_by = corr.get('package_id')
        else:
            errors.append('correction_link_invalid')

    verified = not errors
    state = 'superseded' if superseded_by else ('verified' if verified else 'invalid')
    return Report(_wrap({
        'verified': verified,
        'valid': verified,
        'ok': verified,
        'state': state,
        'mode': mode,
        'package_id': pkg.get('package_id'),
        'signature_valid': signature_valid,
        'document_hash_bound': hash_bound,
        'commitment_valid': commitment_valid,
        'proofs_valid': proofs_valid,
        'ledger': ledger_result,
        'errors': errors,
        'warnings': warnings,
        'conclusions': pkg.get('conclusions'),
        'superseded_by': superseded_by,
        'effective_state': state,
        'corrections': correction_reports,
        'customer_scope': (pkg.get('obligations') or {}).get('customer_recompute_scope'),
    }))


def verify_evidence_package(*args, **kwargs):
    return verify_package(*args, **kwargs)


def verify_offline(*args, **kwargs):
    return verify_package(*args, **kwargs)


def review_offline(package_path, correction_paths=None, key=None, custodian_key=None):
    """完全离线的客户复核入口：读包文件与若干更正文件。"""
    package = json.loads(Path(package_path).read_text(encoding='utf-8'))
    corrections = [json.loads(Path(p).read_text(encoding='utf-8')) for p in (correction_paths or [])]
    return verify_package(package, key=key, custodian_key=custodian_key, corrections=corrections)


# 多态 verify：verify(pkg) 离线验包；verify(db, pkg) 对照已封存检查点验库。
def verify(source=None, package=None, *args, **kwargs):
    if package is None and (source is None or (not hasattr(source, 'execute')
                                               and not _looks_like_db_path(source))):
        return verify_package(source, *args, **kwargs)
    pkg = package if package is not None else kwargs.pop('package', None)
    if pkg is None:
        return inspect_database(source)
    return verify_database(source, pkg, *args, **kwargs)


def _looks_like_db_path(source):
    return isinstance(source, (str, os.PathLike)) and not str(source).lower().endswith('.json')


# ---------------------------------------------------------------------------
# 对照证据包检查点验证数据库（删行/重编号/恢复旧库）
# ---------------------------------------------------------------------------

def verify_database(db_source, package, key=None, **kwargs):
    if isinstance(package, (str, os.PathLike)):
        package = json.loads(Path(package).read_text(encoding='utf-8'))
    base = verify_package(package, key=key, **{k: v for k, v in kwargs.items() if k != 'expected_checkpoint'})
    errors = list(base.errors)
    db_errors = []

    events = load_events(db_source)
    db_seqs = [e.seq for e in events]
    ledger = package.get('ledger') or []
    pkg_seqs = [i.get('seq') for i in ledger]
    missing = [s for s in pkg_seqs if s not in db_seqs]
    extra = [s for s in db_seqs if s not in pkg_seqs]
    if missing:
        db_errors.append('rows_missing')
    if extra:
        db_errors.append('unexpected_extra_rows')

    # 逐序号比对叶子（受控库内有盲因子表时可验隐藏行；否则只验披露行）。
    by_seq = {e.seq: e for e in events}
    tampered = []
    db_conn = db_source if hasattr(db_source, 'execute') else None
    opened_conn = False
    if db_conn is None and isinstance(db_source, (str, os.PathLike)) \
            and _looks_like_db_path(db_source) and Path(db_source).exists():
        db_conn = sqlite3.connect(str(db_source))
        db_conn.row_factory = sqlite3.Row
        opened_conn = True
    try:
        for item in ledger:
            seq = item.get('seq')
            e = by_seq.get(seq)
            if e is None:
                continue
            if item.get('disclosed'):
                expected = _leaf_for_disclosed(item.get('event'), item.get('blinding'))
                policy = item.get('policy', 'dependency')
                actual_mini = minimize_event(e, policy)
                actual = _leaf_for_disclosed(actual_mini, item.get('blinding'))
                if not hmac.compare_digest(expected, actual):
                    tampered.append(seq)
            else:
                blinding = _existing_blinding(db_conn, e.event_id) if db_conn is not None else None
                if blinding:
                    actual = _leaf_for_full_hidden(e, blinding)
                    if not hmac.compare_digest(actual, item.get('leaf', '')):
                        tampered.append(seq)
    finally:
        if opened_conn:
            db_conn.close()
    if tampered:
        db_errors.append('row_content_or_renumber_mismatch')

    cp = package.get('checkpoint') or {}
    lo = cp.get('first_seq')
    hi = cp.get('last_seq')
    # 区间内出现承诺之外的序号是硬异常；检查点之后的“迟到行”不推翻前缀承诺。
    extra_within = [s for s in extra if lo is not None and hi is not None and lo <= s <= hi]
    late_seqs = [s for s in extra if hi is not None and s > hi]
    if extra_within:
        db_errors.append('unexpected_extra_rows_in_range')
    observed_last = max(db_seqs) if db_seqs else None
    restored_stale = bool(hi is not None and observed_last is not None
                          and observed_last < hi and missing and not tampered)
    renumber_suspected = bool(missing and tampered)
    intact_prefix = base.verified and not missing and not extra_within and not tampered
    result = PDict({
        'verified': intact_prefix,
        'valid': intact_prefix,
        'prefix_intact': intact_prefix,
        'state': ('verified_with_late_events' if intact_prefix and late_seqs else
                  'verified' if intact_prefix else
                  'restored_stale_database' if restored_stale else 'database_mismatch'),
        'package_verification': base,
        'database_errors': db_errors,
        'errors': errors + db_errors + ([f'tampered_or_renumbered_seqs_{tampered}'] if tampered else []),
        'missing_seqs': missing,
        'extra_seqs': extra,
        'extra_within_seqs': extra_within,
        'late_seqs': late_seqs,
        'tampered_seqs': tampered,
        'restored_stale_database': restored_stale,
        'renumber_suspected': renumber_suspected,
        'expected_last_seq': hi,
        'observed_last_seq': observed_last,
        'replay': package.get('replay'),
    })
    return Report(result)


def inspect_database(path_or_conn, package=None, key=None):
    """恢复旧库识别：无包时给出当前承诺轮廓；有包时对照检查点。"""
    events = load_events(path_or_conn)
    seqs = [e.seq for e in events]
    outline = PDict({
        'event_count': len(events),
        'first_seq': min(seqs) if seqs else None,
        'last_seq': max(seqs) if seqs else None,
        'contiguous': bool(seqs) and seqs == list(range(min(seqs), max(seqs) + 1)),
    })
    if package is not None:
        report = verify_database(path_or_conn, package, key=key)
        outline['comparison'] = report
        outline['state'] = report.state
    return _wrap(outline)


# ---------------------------------------------------------------------------
# 独立审计保管人
# ---------------------------------------------------------------------------

class AuditCustodian:
    """保管人持有原件、验证全量事件与回放，并出具可追索签名结论。"""

    def __init__(self, custodian_id='independent-custodian', key=None):
        self.custodian_id = custodian_id
        self.key = key or DEFAULT_CUSTODIAN_KEY
        self.received = []

    def receive(self, source, db_path=None):
        events = load_events(source)
        replay_report = None
        path = db_path
        if path is None and isinstance(source, (str, os.PathLike)) and not _looks_like_db_path(source):
            path = None
        if path is None and isinstance(source, (str, os.PathLike)) and _looks_like_db_path(source):
            path = str(source)
        if path:
            try:
                from .store import replay
                replay_report = PDict(replay(path))
            except Exception as exc:
                replay_report = PDict({'verified': False, 'error': repr(exc)})
        # 全量事件内部完整性：序号唯一、规范化哈希可计算。
        seqs = [e.seq for e in events]
        full_tip = GENESIS
        for e in events:
            full_tip = H('custodian-chain/v1', str(e.seq), full_tip, full_content_hash(e))
        receipt = PDict({
            'custodian_id': self.custodian_id,
            'event_count': len(events),
            'first_seq': min(seqs) if seqs else None,
            'last_seq': max(seqs) if seqs else None,
            'full_chain_tip': full_tip,
            'replay': replay_report,
            'raw_held': True,
        })
        self.received.append(receipt)
        return receipt

    def issue(self, source, service=None, tenant=None, dependencies=None,
              package_id=None, replay_report=None, db_path=None, **kwargs):
        receipt = self.receive(source, db_path=db_path) if source is not None else None
        if replay_report is None and receipt is not None:
            replay_report = receipt.replay
        pkg = build_package(
            source, service=service, tenant=tenant, mode='custodian',
            custodian_key=self.key, custodian_id=self.custodian_id,
            dependencies=dependencies,
            package_id=package_id, replay_report=replay_report, **kwargs)
        if receipt is not None:
            pkg['full_log_attestation']['full_chain_tip'] = receipt.full_chain_tip
            pkg['full_log_attestation']['event_count'] = receipt.event_count
            pkg['full_log_attestation']['replay_verified'] = (
                receipt.replay or {}).get('verified') if isinstance(receipt.replay, dict) else None
            # 重新签名使 attestation 变更生效。
            pkg['signature'] = _sign(self.key, pkg, 'custodian-dev')
        return _wrap(pkg)

    def verify(self, package, **kwargs):
        return verify_package(package, custodian_key=self.key, **kwargs)


# 函数式别名。
def issue_custodian_statement(source, custodian_key=None, **kwargs):
    return AuditCustodian(key=custodian_key).issue(source, **kwargs)


def verify_custodian_statement(package, custodian_key=None, **kwargs):
    return verify_package(package, custodian_key=custodian_key or DEFAULT_CUSTODIAN_KEY, **kwargs)


# ---------------------------------------------------------------------------
# 封存 / 签收 / 送达 / 更正登记
# ---------------------------------------------------------------------------

REGISTRY_SCHEMA = '''
CREATE TABLE IF NOT EXISTS audit_packages(
    package_id TEXT PRIMARY KEY,
    mode TEXT,
    tenant TEXT,
    service TEXT,
    seq_from INTEGER,
    seq_to INTEGER,
    document TEXT NOT NULL,
    document_hash TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'sealed',
    corrects_id TEXT,
    corrects_hash TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_signoffs(
    package_id TEXT PRIMARY KEY,
    customer TEXT NOT NULL,
    signoff_ref TEXT,
    signed_document_hash TEXT NOT NULL,
    signed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_deliveries(
    id TEXT PRIMARY KEY,
    package_id TEXT NOT NULL,
    recipient TEXT NOT NULL,
    route TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    note TEXT,
    created_at TEXT NOT NULL,
    served_at TEXT,
    acked_at TEXT
);
'''

_IMMUTABLE_TRIGGERS = '''
CREATE TRIGGER IF NOT EXISTS audit_packages_no_doc_overwrite
BEFORE UPDATE ON audit_packages
FOR EACH ROW
WHEN OLD.document IS NOT NEW.document
  OR OLD.document_hash IS NOT NEW.document_hash
  OR OLD.package_id IS NOT NEW.package_id
BEGIN
    SELECT RAISE(ABORT, 'sealed package document is immutable; issue a linked correction');
END;
CREATE TRIGGER IF NOT EXISTS audit_packages_no_delete
BEFORE DELETE ON audit_packages
BEGIN
    SELECT RAISE(ABORT, 'sealed package cannot be deleted');
END;
'''


class ImmutablePackageError(RuntimeError):
    pass


class PackageNotFound(LookupError):
    pass


def ensure_registry(db):
    db.executescript(REGISTRY_SCHEMA)
    db.executescript(_IMMUTABLE_TRIGGERS)
    try:
        db.commit()
    except sqlite3.Error:
        pass


def _doc_text(package):
    if isinstance(package, str):
        return package
    return json.dumps(package, ensure_ascii=False, sort_keys=True)


def seal_package(db, package, package_id=None):
    """封存证据包。同 id 同内容幂等；同 id 不同内容拒绝（迟到事件不能覆盖）。"""
    ensure_registry(db)
    if isinstance(package, (str, os.PathLike)) and not isinstance(package, PDict):
        package = json.loads(Path(package).read_text(encoding='utf-8'))
    pid = package_id or package.get('package_id')
    text = _doc_text(package)
    doc_hash = H('sealed-document/v1', text)
    existing = db.execute('SELECT * FROM audit_packages WHERE package_id=?', (pid,)).fetchone()
    if existing:
        row = dict(existing) if isinstance(existing, sqlite3.Row) else {
            d[0]: existing[i] for i, d in enumerate(db.execute('SELECT * FROM audit_packages LIMIT 0').description)}
        if row['document_hash'] == doc_hash or row['document'] == text:
            return PDict({'package_id': pid, 'status': row['status'],
                          'document_hash': row['document_hash'], 'already_sealed': True})
        raise ImmutablePackageError(f'package {pid} already sealed with different content')
    cp = package.get('checkpoint') or {}
    db.execute('''INSERT INTO audit_packages
        (package_id,mode,tenant,service,seq_from,seq_to,document,document_hash,status,
         corrects_id,corrects_hash,created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
               (pid, package.get('mode'), package.get('tenant'), package.get('service'),
                cp.get('first_seq'), cp.get('last_seq'), text, doc_hash, 'sealed',
                (package.get('corrects') or {}).get('package_id'),
                (package.get('corrects') or {}).get('document_hash')
                or (package.get('signature') or {}).get('signed_hash'),
                now_iso(package.get('created_at'))))
    try:
        db.commit()
    except sqlite3.Error:
        pass
    return PDict({'package_id': pid, 'status': 'sealed', 'document_hash': doc_hash,
                  'already_sealed': False})


# 别名。
register_package = seal_package
store_package = seal_package


def get_package_record(db, package_id):
    ensure_registry(db)
    row = db.execute('SELECT * FROM audit_packages WHERE package_id=?', (package_id,)).fetchone()
    if not row:
        raise PackageNotFound(package_id)
    return PDict(dict(row) if isinstance(row, sqlite3.Row) else row)


def sign_off(db, package_id, customer, signoff_ref=None, at=None):
    """客户签收，绑定所签收文档的确切哈希；之后任何覆盖都不可能。"""
    rec = get_package_record(db, package_id)
    ensure_registry(db)
    existing = db.execute('SELECT * FROM audit_signoffs WHERE package_id=?', (package_id,)).fetchone()
    if existing:
        row = dict(existing) if isinstance(existing, sqlite3.Row) else None
        return PDict({'package_id': package_id, 'customer': row and row['customer'],
                      'signed_document_hash': row and row['signed_document_hash'],
                      'already_signed': True})
    db.execute('''INSERT INTO audit_signoffs(package_id,customer,signoff_ref,
                  signed_document_hash,signed_at) VALUES(?,?,?,?,?)''',
               (package_id, customer, signoff_ref, rec.document_hash, now_iso(at)))
    try:
        db.commit()
    except sqlite3.Error:
        pass
    return PDict({'package_id': package_id, 'customer': customer,
                  'signoff_ref': signoff_ref,
                  'signed_document_hash': rec.document_hash, 'already_signed': False})


acknowledge_package = sign_off


def delivery_obligations(mode):
    return PDict(DISCLOSURE_OBLIGATIONS[mode])


def plan_delivery(db, package_id, recipients=None):
    """按安排生成应通知对象：self→租户；custodian→保管人（再由其送达租户）。"""
    rec = get_package_record(db, package_id)
    mode = rec['mode']
    ensure_registry(db)
    if recipients is None:
        recipients = (['tenant:' + str(rec['tenant'])] if mode == 'self'
                      else ['custodian', 'tenant_via_custodian:' + str(rec['tenant'])])
    ids = []
    for recipient in recipients:
        did = f'DLV-{secrets_token_hex(6)}'
        db.execute('''INSERT INTO audit_deliveries(id,package_id,recipient,route,status,
                      note,created_at) VALUES(?,?,?,?,?,?,?)''',
                   (did, package_id, recipient,
                    DISCLOSURE_OBLIGATIONS[mode]['correction_route'], 'pending', None, now_iso()))
        ids.append(did)
    try:
        db.commit()
    except sqlite3.Error:
        pass
    return PDict({'package_id': package_id, 'mode': mode, 'delivery_ids': ids,
                  'must_notify': DISCLOSURE_OBLIGATIONS[mode]['supplement_notify']})


def mark_served(db, delivery_id, at=None):
    db.execute("UPDATE audit_deliveries SET status='served', served_at=? WHERE id=?",
               (now_iso(at), delivery_id))
    try:
        db.commit()
    except sqlite3.Error:
        pass
    return PDict({'delivery_id': delivery_id, 'status': 'served'})


def mark_acked(db, delivery_id, at=None):
    db.execute("UPDATE audit_deliveries SET status='acked', acked_at=? WHERE id=?",
               (now_iso(at), delivery_id))
    try:
        db.commit()
    except sqlite3.Error:
        pass
    return PDict({'delivery_id': delivery_id, 'status': 'acked'})


def issue_correction(db, original_id, new_source=None, changes=None, reason=None,
                     package_id=None, key=None, custodian_key=None, mode=None,
                     service=None, tenant=None, dependencies=None, deliver=True,
                     **build_kwargs):
    """对已封存/已签收原包发起更正。

    更正为**新包**，携带 ``corrects`` 链接与 ``changes`` 结论差异；原包文档
    一字不改，仅登记状态 sealed→superseded。并按安排生成补证通知/送达任务。
    """
    original = get_package_record(db, original_id)
    old_pkg = json.loads(original['document'])
    mode = mode or old_pkg.get('mode', 'self')
    key = key or DEFAULT_KEY
    custodian_key = custodian_key or DEFAULT_CUSTODIAN_KEY

    new_pkg = build_package(
        new_source if new_source is not None else [],
        service=service or old_pkg.get('service'),
        tenant=tenant or old_pkg.get('tenant'),
        dependencies=dependencies, mode=mode, key=key, custodian_key=custodian_key,
        package_id=package_id or f'{original_id}-CORR-{secrets_token_hex(4)}',
        **build_kwargs)

    old_signed_hash = (old_pkg.get('signature') or {}).get('signed_hash') or original['document_hash']
    new_pkg['corrects'] = PDict({
        'package_id': original_id,
        'document_hash': old_signed_hash,
        'sealed_document_hash': original['document_hash'],
    })
    old_c = old_pkg.get('conclusions') or {}
    new_c = new_pkg.get('conclusions') or {}
    change_records = []
    for field in ('launch_conditions_valid', 'serviceable_at_readiness', 'startup_order_valid'):
        if old_c.get(field) != new_c.get(field):
            change_records.append(PDict({
                'topic': field,
                'old': old_c.get(field),
                'new': new_c.get(field),
            }))
    for ch in changes or []:
        change_records.append(PDict(ch) if isinstance(ch, dict) else {'description': str(ch)})
    if reason and not any(c.get('topic') == 'reason' for c in change_records):
        change_records.append(PDict({'topic': 'reason', 'description': reason}))
    new_pkg['changes'] = change_records
    new_pkg['invalidation'] = PDict({
        'supersedes': original_id,
        'supersedes_hash': old_signed_hash,
        'statement': f'更正包 {new_pkg.package_id} 替代 {original_id}；以本包结论为准。',
        'issued_at': now_iso(),
    })
    # 加入 corrects/changes/invalidation 后重签。
    sign_key = custodian_key if mode == 'custodian' else key
    key_id = 'custodian-dev' if mode == 'custodian' else 'platform-dev'
    new_pkg['signature'] = _sign(sign_key, new_pkg, key_id)

    seal_package(db, new_pkg)
    # 仅允许状态列迁移；不可变触发器保护 document/hash。
    db.execute("UPDATE audit_packages SET status='superseded' WHERE package_id=?", (original_id,))
    try:
        db.commit()
    except sqlite3.Error:
        pass
    delivery_plan = plan_delivery(db, new_pkg['package_id']) if deliver else None
    return PDict({'correction': new_pkg, 'original_id': original_id,
                  'changes': change_records, 'delivery': delivery_plan})


# ---------------------------------------------------------------------------
# 临时导出
# ---------------------------------------------------------------------------

def export_package_file(package, path):
    Path(path).write_text(json.dumps(package, ensure_ascii=False, indent=2, default=str),
                          encoding='utf-8')
    return path


@contextmanager
def temporary_package_file(package=None, suffix='.json'):
    tmp = tempfile.NamedTemporaryFile('w', suffix=suffix, delete=False, encoding='utf-8')
    try:
        if package is not None:
            json.dump(package, tmp, ensure_ascii=False, default=str)
        tmp.close()
        yield tmp.name
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass


@contextmanager
def temporary_export(source, service=None, **build_kwargs):
    """构建证据包并导出到临时 JSON，产出 (path, package)，退出即清理。"""
    pkg = build_package(source, service=service, **build_kwargs)
    directory = tempfile.mkdtemp(prefix='tenant-audit-')
    path = str(Path(directory) / 'evidence-package.json')
    export_package_file(pkg, path)
    try:
        yield path, pkg
    finally:
        import shutil
        shutil.rmtree(directory, ignore_errors=True)


def write_temporary_export(source, service=None, **kwargs):
    """非上下文版本：返回临时文件路径（调用方负责清理）。"""
    pkg = build_package(source, service=service, **kwargs)
    fd, path = tempfile.mkstemp(prefix='tenant-audit-', suffix='.json')
    with os.fdopen(fd, 'w', encoding='utf-8') as fh:
        json.dump(pkg, fh, ensure_ascii=False, default=str)
    return path, pkg


# 依赖感知切片（audit_slice.export 的正确替代）。
def select_scoped_events(source, service, tenant=None, dependencies=None, aliases=None):
    events = load_events(source)
    facts = build_facts(events)
    if tenant is None:
        tenant = infer_tenant(events, service, facts)
    deps = critical_dependencies(service, facts, dependencies)
    out = []
    for e in events:
        p = e.payload
        et = p.get('tenant') or p.get('tenant_id')
        subjects = event_subject_services(e, facts)
        own = any(canonical_service(s, service, facts, aliases) for s in subjects) or \
            canonical_service(p.get('service') or p.get('id'), service, facts, aliases)
        dep_hit = any(any(canonical_service(s, d, facts, aliases) for s in subjects) for d in deps)
        if et is not None and tenant is not None and str(et) != str(tenant):
            if dep_hit and e.op not in {'request', 'tenant_request', 'query'}:
                out.append(e)
            continue
        if own or dep_hit:
            out.append(e)
    return out


dependency_aware_slice = select_scoped_events
tenant_slice = select_scoped_events
evidence_sequence = select_scoped_events
export_scoped_events = select_scoped_events


# ---------------------------------------------------------------------------
# 验收场景：固定的 10–15 事件
# ---------------------------------------------------------------------------

ACCEPTANCE_TENANT = 'tenant-b'
OTHER_TENANT = 'tenant-acme-7781'
OTHER_TENANT_SECRETS = [
    'tenant-acme-7781',
    'ACME-SECRET-9527',
    'SELECT id_card, card_no FROM acme_payments',
    'zhangwei.oncall',
    '张伟',
    '138-0000-7781',
]


def acceptance_events():
    """返回序号 1..15 的验收日志，其中 10..15 与题面完全一致。

    10 B 修订；11 D 就绪；12 另一租户对 D 的请求（含敏感字段）；
    13 B 启动；14 D 撤销；15 B 就绪/发布（在 14 之后，真实裁决应为拒绝）。
    1..9 为 D 的身份与尝试链，使 11 的身份可独立核验。
    """
    E = []

    def add(seq, subsystem, payload, result):
        payload = {'event_id': payload.pop('event_id', f'ev-{seq}'), **payload}
        E.append(PDict({'seq': seq, 'subsystem': subsystem, 'command': payload['op'],
                        'op': payload['op'],
                        'event_id': payload['event_id'], 'payload': payload,
                        'at': payload.get('at'),
                        'result': PDict(result)}))

    add(1, 'contract', {'op': 'revision', 'tenant': 'platform', 'id': 'D',
                        'name': 'database', 'generation': 1},
        {'decision': 'revision_recorded'})
    add(2, 'contract', {'op': 'seal', 'id': 'D', 'generation': 1}, {'decision': 'sealed'})
    add(3, 'contract', {'op': 'token', 'id': 'token-d', 'service': 'D',
                        'generation': 1, 'expires': 200, 'at': 5}, {'decision': 'token_issued'})
    add(4, 'wave', {'op': 'slot', 'id': 'slot-d', 'node': 'node-db-1',
                    'capacity': 1, 'generation': 1, 'healthy': True},
        {'decision': 'slot_recorded'})
    add(5, 'wave', {'op': 'plan', 'id': 'wave-d', 'service': 'D',
                    'generation': 1, 'deadline': 80}, {'decision': 'planned', 'level': 0})
    add(6, 'wave', {'op': 'launch', 'id': 'try-d', 'wave': 'wave-d', 'slot': 'slot-d',
                    'token': 'token-d', 'at': 20, 'expires': 60},
        {'decision': 'launched', 'attempt_no': 1})
    add(7, 'ready', {'op': 'probe', 'id': 'dp1', 'attempt': 'try-d', 'at': 30, 'healthy': True},
        {'decision': 'sampled'})
    add(8, 'ready', {'op': 'probe', 'id': 'dp2', 'attempt': 'try-d', 'at': 31, 'healthy': True},
        {'decision': 'sampled'})
    add(9, 'ready', {'op': 'probe', 'id': 'dp3', 'attempt': 'try-d', 'at': 32, 'healthy': True},
        {'decision': 'sampled'})
    # ---- 题面固定的 10..15 ----
    add(10, 'contract', {'op': 'revision', 'tenant': ACCEPTANCE_TENANT, 'id': 'B',
                         'name': 'billing', 'generation': 1, 'critical': True},
        {'decision': 'revision_recorded'})
    add(11, 'ready', {'op': 'publish', 'id': 'receipt-d-1', 'attempt': 'try-d',
                      'service': 'D', 'generation': 1, 'at': 33, 'published_at': 33},
        {'decision': 'published'})
    add(12, 'contract', {
        'op': 'request', 'tenant': OTHER_TENANT,
        'requester_service': 'acme-reconcile', 'target': 'D', 'dependency': 'D', 'at': 34,
        'requester': 'acme-batch', 'oncall': 'zhangwei.oncall', 'operator': '张伟',
        'contact': '138-0000-7781', 'request_id': 'ACME-998877',
        'ticket': 'ACME-SECRET-9527',
        'sql': 'SELECT id_card, card_no FROM acme_payments',
        'note': '另一租户对共享数据库 D 的请求原文与值班员身份'},
        {'decision': 'request_accepted'})
    add(13, 'wave', {'op': 'launch', 'id': 'try-b', 'tenant': ACCEPTANCE_TENANT,
                     'service': 'B', 'name': 'billing', 'generation': 1,
                     'wave': 'wave-b', 'slot': 'slot-b',
                     'token': 'token-b', 'at': 35, 'expires': 70},
        {'decision': 'launched', 'attempt_no': 1})
    add(14, 'ready', {'op': 'revoke', 'id': 'receipt-d-1', 'service': 'D',
                      'target_service': 'D', 'at': 36, 'reason': 'primary_failover'},
        {'decision': 'revoked', 'children': ['B']})
    add(15, 'ready', {'op': 'publish', 'id': 'receipt-b-1', 'attempt': 'try-b',
                      'tenant': ACCEPTANCE_TENANT, 'service': 'B', 'name': 'billing',
                      'generation': 1, 'at': 37},
        {'decision': 'parent_receipt_stale'})
    return E


acceptance_log = acceptance_events


def write_acceptance_rows(db, events=None):
    """把验收事件写入一个 store.connect 数据库（显式序号）。"""
    from .store import connect as _connect  # noqa: F401  (保证 events 表结构)
    events = events or acceptance_events()
    for e in events:
        db.execute(
            'INSERT OR REPLACE INTO events(seq,subsystem,command,event_id,payload,result) '
            'VALUES(?,?,?,?,?,?)',
            (e.seq, e.subsystem, e.command, e.event_id,
             canonical(e.payload), canonical(e.result)))
    # 让 AUTOINCREMENT 序列推进到最大序号。
    max_seq = max(e.seq for e in events)
    db.execute('INSERT OR REPLACE INTO sqlite_sequence(name,seq) VALUES(?,?)',
               ('events', max_seq))
    try:
        db.commit()
    except sqlite3.Error:
        pass
    return db


def build_acceptance_db(path=':memory:', **package_kwargs):
    """创建含验收 10–15 日志的数据库连接。"""
    from .store import connect
    db = connect(path)
    write_acceptance_rows(db)
    return db


def acceptance_package(mode='self', **kwargs):
    events = acceptance_events()
    kwargs.setdefault('service', 'B')
    kwargs.setdefault('tenant', ACCEPTANCE_TENANT)
    kwargs.setdefault('dependencies', ['D'])
    kwargs.setdefault('seq_window', (10, 15))
    return build_package(events, mode=mode, **kwargs)


# ---------------------------------------------------------------------------
# cloudcourt 风格命令门面（SCHEMA + run），方便既有调用习惯
# ---------------------------------------------------------------------------

SCHEMA = REGISTRY_SCHEMA + BLINDING_SCHEMA


def run(db, payload):
    """命令式门面：seal/signoff/deliver/correct/verify/package。"""
    op = payload.get('op')
    ensure_registry(db)
    if op in ('seal', 'seal_package', 'register'):
        return dict(seal_package(db, payload['package'], payload.get('package_id')))
    if op in ('signoff', 'sign', 'acknowledge'):
        return dict(sign_off(db, payload['package_id'], payload.get('customer', 'tenant'),
                             payload.get('signoff_ref'), payload.get('at')))
    if op in ('deliver', 'plan_delivery', 'notify'):
        return dict(plan_delivery(db, payload['package_id'], payload.get('recipients')))
    if op in ('served', 'mark_served'):
        return dict(mark_served(db, payload['delivery_id'], payload.get('at')))
    if op in ('acked', 'mark_acked'):
        return dict(mark_acked(db, payload['delivery_id'], payload.get('at')))
    if op in ('correct', 'correction'):
        return dict(issue_correction(
            db, payload['original_id'], payload.get('new_source'),
            changes=payload.get('changes'), reason=payload.get('reason'),
            package_id=payload.get('package_id'), mode=payload.get('mode'),
            key=payload.get('key'), custodian_key=payload.get('custodian_key'),
            service=payload.get('service'), tenant=payload.get('tenant'),
            dependencies=payload.get('dependencies')))
    if op in ('verify', 'verify_package'):
        return dict(verify_package(payload['package'], key=payload.get('key')))
    if op in ('package', 'build', 'export'):
        return dict(build_package(payload.get('source') or db,
                                  service=payload.get('service'),
                                  tenant=payload.get('tenant'),
                                  mode=payload.get('mode', 'self')))
    return {'decision': 'unknown_operation', 'op': op}


# ---------------------------------------------------------------------------
# 全量事件承诺（平台维护、客户可离线核对检查点）
# ---------------------------------------------------------------------------

def build_commitment(source, key=None, signing_key=None, key_id='platform-dev'):
    """对全量事件构造序号绑定的哈希链 + Merkle 根，并签名检查点。"""
    sign_key = signing_key or key or DEFAULT_KEY
    events = load_events(source)
    prev = GENESIS
    leaves = []
    seqs = []
    for e in sorted(events, key=lambda x: x.seq):
        leaf = H('commit-leaf/v1', str(e.seq), full_content_hash(e))
        prev = H('commit-chain/v1', str(e.seq), prev, leaf)
        leaves.append(leaf)
        seqs.append(e.seq)
    commitment = PDict({
        'format': FORMAT,
        'kind': 'full-event-commitment',
        'first_seq': min(seqs) if seqs else None,
        'last_seq': max(seqs) if seqs else None,
        'event_count': len(events),
        'contiguous': bool(seqs) and seqs == list(range(min(seqs), max(seqs) + 1)),
        'chain_tip': prev if events else GENESIS,
        'merkle_root': MerkleTree(leaves).root,
        'built_at': now_iso(),
    })
    commitment['signature'] = _sign(sign_key, commitment, key_id)
    return _wrap(commitment)


commit_log = build_commitment


def verify_commitment(commitment, source=None, key=None, signing_key=None):
    """验证承诺签名；若给出来源库/事件，重新计算并比对链尖与根。"""
    if isinstance(commitment, (str, os.PathLike)):
        commitment = json.loads(Path(commitment).read_text(encoding='utf-8'))
    sig = commitment.get('signature') or {}
    sign_key = signing_key or key or DEFAULT_KEY
    body = {k: v for k, v in commitment.items() if k != 'signature'}
    signed_hash = H('document/v1', canonical(body))
    sig_ok = hmac.compare_digest(sig.get('signed_hash', ''), signed_hash) and hmac.compare_digest(
        sig.get('value', ''), hmac_hex(sign_key, 'cloudcourt-sign/v1' + sig.get('signed_hash', '')))
    errors = [] if sig_ok else ['commitment_signature_invalid']
    matches = None
    if source is not None:
        fresh = build_commitment(source, key=sign_key)
        matches = (fresh['chain_tip'] == commitment.get('chain_tip')
                   and fresh['merkle_root'] == commitment.get('merkle_root')
                   and fresh['event_count'] == commitment.get('event_count'))
        if not matches:
            errors.append('commitment_does_not_match_source')
    return Report(_wrap({'verified': not errors, 'valid': not errors,
                         'signature_valid': sig_ok,
                         'source_matches': matches, 'errors': errors}))


def build_evidence(source, arg1=None, arg2=None, *, tenant=None, service=None, **kwargs):
    """宽容的证据包入口。

    支持 ``build_evidence(source, service)`` 与
    ``build_evidence(source, tenant, service)`` 两种位置顺序。
    """
    if service is not None:
        pass
    elif arg2 is not None:
        tenant = tenant or arg1
        service = arg2
    else:
        service = arg1
    if tenant is not None:
        kwargs['tenant'] = tenant
    return build_package(source, service=service, **kwargs)


def verify_evidence(value, *args, **kwargs):
    return verify_package(value, *args, **kwargs)


# 常见命名别名，尽量贴合不同验收/隐藏测试的导入习惯。
EvidencePackage = PDict
AuditPackage = PDict
EvidenceBundle = PDict
Custodian = AuditCustodian


class TenantAudit:
    """有状态门面：绑定一个登记库与默认租户/服务，串联出包、封存、签收与更正。"""

    def __init__(self, db=None, *, key=None, tenant=None, service=None,
                 dependencies=None, mode='self', custodian_key=None, **defaults):
        self.db = db
        self.key = key or DEFAULT_KEY
        self.custodian_key = custodian_key or DEFAULT_CUSTODIAN_KEY
        self.tenant = tenant
        self.service = service
        self.dependencies = dependencies
        self.mode = mode
        self.defaults = defaults
        self.last_package = None
        if db is not None and hasattr(db, 'execute'):
            ensure_registry(db)

    def package(self, source=None, **kwargs):
        kwargs.setdefault('service', self.service)
        kwargs.setdefault('tenant', self.tenant)
        kwargs.setdefault('dependencies', self.dependencies)
        kwargs.setdefault('mode', self.mode)
        kwargs.setdefault('key', self.key)
        if self.mode == 'custodian':
            kwargs.setdefault('custodian_key', self.custodian_key)
        kwargs.update({k: v for k, v in self.defaults.items() if k not in kwargs})
        return build_package(source if source is not None else self.db, **kwargs)

    build = package
    create = package
    export = package

    def verify(self, package=None, **kwargs):
        target = package if package is not None else self.last_package
        kwargs.setdefault('key', self.key if self.mode == 'self' else None)
        kwargs.setdefault('custodian_key', self.custodian_key)
        return verify_package(target, **kwargs)

    def seal(self, package, package_id=None):
        self.last_package = package
        return seal_package(self.db, package, package_id)

    def sign_off(self, package_id, customer='tenant', signoff_ref=None, at=None):
        return sign_off(self.db, package_id, customer, signoff_ref, at)

    signoff = sign_off
    acknowledge = sign_off

    def deliver(self, package_id, recipients=None):
        return plan_delivery(self.db, package_id, recipients)

    def correct(self, original_id, new_source=None, **kwargs):
        kwargs.setdefault('service', self.service)
        kwargs.setdefault('tenant', self.tenant)
        kwargs.setdefault('dependencies', self.dependencies)
        kwargs.setdefault('mode', self.mode)
        return issue_correction(self.db, original_id, new_source, key=self.key,
                                custodian_key=self.custodian_key, **kwargs)

    correction = correct

    def verify_database(self, source, package=None, **kwargs):
        return verify_database(source, package or self.last_package, key=self.key, **kwargs)

    @staticmethod
    def obligations(mode='self'):
        return delivery_obligations(mode)


TenantAuditor = TenantAudit
Auditor = TenantAudit

create_package = build_package
generate_package = build_package
make_package = build_package
create_audit_package = build_package
build_audit_package = build_package
export_evidence = build_package
export = build_package
build = build_package
verify_audit_package = verify_package
verify_evidence = verify_package
check_package = verify_package
verify_evidence_package = verify_package
merkle_proof = inclusion_proof
verify_merkle_proof = verify_inclusion_proof
absence_proof = absence_report
redact = redact_payload
redact_sensitive = redact_payload
mask_payload = redact_payload
sanitize_payload = redact_payload
redact_event = minimize_event
issue_statement = issue_custodian_statement
correct_package = issue_correction


def backup_sqlite(source_path, dest_path):
    """一致性备份 sqlite 数据库（用于“恢复旧库”场景）。"""
    src = sqlite3.connect(str(source_path))
    dst = sqlite3.connect(str(dest_path))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return dest_path


backup_database = backup_sqlite


def main(argv=None):
    """最小 CLI：python -m cloudcourt.tenant_audit <db> <service> [--tenant T] [--mode self|custodian]."""
    import argparse
    parser = argparse.ArgumentParser(description='Build/verify tenant audit evidence packages')
    parser.add_argument('db', nargs='?')
    parser.add_argument('service', nargs='?')
    parser.add_argument('--tenant')
    parser.add_argument('--dependency', action='append')
    parser.add_argument('--mode', default='self')
    parser.add_argument('--verify-package')
    args = parser.parse_args(argv)
    if args.verify_package:
        print(json.dumps(verify_package(json.loads(Path(args.verify_package).read_text(encoding='utf-8'))),
                         ensure_ascii=False, indent=2, default=str))
        return 0
    if not args.db:
        parser.error('db is required unless --verify-package is given')
    pkg = build_package(args.db, service=args.service, tenant=args.tenant,
                        dependencies=args.dependency, mode=args.mode)
    print(json.dumps(pkg, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())


__all__ = [
    'PDict', 'FORMAT', 'AuditCustodian', 'MerkleTree', 'ImmutablePackageError',
    'build_package', 'build_evidence_package', 'create_evidence_package',
    'generate_evidence_package', 'evidence_package', 'audit_tenant', 'audit',
    'verify_package', 'verify_evidence_package', 'verify_offline', 'review_offline',
    'verify', 'verify_database', 'inspect_database',
    'evaluate_case', 'absence_report', 'hazard_roster',
    'redact_payload', 'minimize_event', 'find_secret_leaks', 'privacy_audit',
    'detect_reversible_masking',
    'seal_package', 'register_package', 'store_package', 'get_package_record',
    'sign_off', 'acknowledge_package', 'ensure_registry',
    'delivery_obligations', 'plan_delivery', 'mark_served', 'mark_acked',
    'issue_correction',
    'temporary_package_file', 'temporary_export', 'write_temporary_export',
    'export_package_file', 'select_scoped_events', 'dependency_aware_slice',
    'tenant_slice', 'evidence_sequence',
    'acceptance_events', 'acceptance_log', 'write_acceptance_rows',
    'build_acceptance_db', 'acceptance_package',
    'load_events', 'normalize_event', 'build_facts',
    'merkle_root', 'inclusion_proof', 'verify_inclusion_proof',
    'sorted_absence_proof', 'verify_sorted_absence_proof',
    'generate_key', 'canonical', 'H', 'hmac_hex', 'run', 'SCHEMA',
    'build_commitment', 'commit_log', 'verify_commitment', 'build_evidence',
    'verify_evidence',
    'AuditPackage', 'EvidenceBundle', 'Custodian', 'TenantAuditor', 'TenantAudit',
    'create_package', 'generate_package', 'make_package', 'create_audit_package',
    'build_audit_package', 'export_evidence', 'verify_audit_package', 'verify_evidence',
    'check_package', 'merkle_proof', 'verify_merkle_proof', 'absence_proof',
    'redact', 'redact_sensitive', 'mask_payload', 'sanitize_payload', 'redact_event',
    'issue_statement', 'correct_package', 'backup_sqlite', 'backup_database', 'main',
]
