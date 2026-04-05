import { motion, AnimatePresence } from 'framer-motion'

export default function Modal({ show, title, body, onCancel, onConfirm, confirmLabel = 'Delete', confirmClass = 'btn-danger' }) {
  return (
    <AnimatePresence>
      {show && (
        <motion.div
          className="modal-overlay"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          onClick={onCancel}
        >
          <motion.div
            className="modal-box"
            initial={{ opacity: 0, scale: 0.92, y: 12 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.92, y: 12 }}
            transition={{ type: 'spring', damping: 22, stiffness: 300 }}
            onClick={e => e.stopPropagation()}
          >
            <div className="modal-title">{title}</div>
            <div className="modal-body">{body}</div>
            <div className="modal-actions">
              <button className="btn btn-ghost" onClick={onCancel}>Cancel</button>
              <button className={`btn ${confirmClass}`} onClick={onConfirm}>{confirmLabel}</button>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  )
}
