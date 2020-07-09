use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use std::{sync::atomic::{Ordering::Relaxed, AtomicBool}, thread, time::Duration};

#[pyfunction]
fn rust_side_worker() {
    static STARTED: AtomicBool = AtomicBool::new(false);

    if STARTED.compare_and_swap(false, true, Relaxed) {
        return;
    }

    let num_threads = num_cpus::get().max(1);

    for _ in 0..num_threads {
        thread::spawn(|| smol::run(futures::future::pending::<()>()));
    }
}

#[pyclass]
struct EventCompleter {
    s: async_channel::Sender<PyObject>,
}

#[pymethods]
impl EventCompleter {
    #[call]
    #[args(val)]
    fn __call__(&self, val: PyObject) -> PyResult<()> {
        let _ = self.s.try_send(val);

        Ok(())
    }
}

async fn wait_for_py_coro_impl(loop_: PyObject, coro: PyObject) -> PyResult<PyObject> {
    let r = {
        let gil = Python::acquire_gil();
        let py = gil.python();

        let (s, r) = async_channel::bounded(1);

        let fut = loop_.call_method1(py, "create_task", (coro,))?;

        let callback = PyCell::new(py, EventCompleter { s })?;

        fut.call_method1(py, "add_done_callback", (callback,))?;

        r
    };

    r.recv().await.map_err(|_| pyo3::exceptions::RuntimeError::py_err("future dropped"))
}

fn set_fut_result(loop_: PyObject, fut: PyObject, res: PyObject) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let sr = fut.getattr(py, "set_result")?;

    loop_.call_method1(py, "call_soon_threadsafe", (sr, res))?;

    Ok(())
}

fn get_loop(py: Python) -> PyResult<PyObject> {
    let asyncio = PyModule::import(py, "asyncio")?;
    let loop_ = asyncio.call0("get_running_loop")?;

    Ok(loop_.into())
}

#[pyfunction]
fn wait_for_py_coro(fut: PyObject) -> PyResult<()> {
    let loop_ = {
        let gil = Python::acquire_gil();
        let py = gil.python();
        get_loop(py)?
    };

    smol::Task::spawn(async move {
        let r = wait_for_py_coro_impl(loop_, fut).await;
        match r {
            Ok(v) => {
                println!("wfpc got Ok: {:?}", v);
            }
            Err(e) => {
                eprintln!("waiting on python coro failed");
                let gil = Python::acquire_gil();
                let py = gil.python();
                e.print(py);
            }
        }
    }).detach();

    Ok(())
}

#[pyfunction]
fn delay_test(delay: u64, result: PyObject) -> PyResult<PyObject> {
    let (fut, res_fut, loop_): (PyObject, PyObject, PyObject) = {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let loop_ = get_loop(py)?;
        let fut: PyObject = loop_.call_method0(py, "create_future")?.into();
        (fut.clone_ref(py), fut, loop_.into())
    };

    smol::Task::spawn(async move {
        smol::Timer::after(Duration::from_secs(delay)).await;

        println!("setting result");
        if let Err(e) = set_fut_result(loop_, fut, result) {
            let gil = Python::acquire_gil();
            let py = gil.python();
            e.print(py);
        }
        println!("done");
    }).detach();

    Ok(res_fut)
}


#[pymodule]
fn async_py_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(rust_side_worker))?;
    m.add_wrapped(wrap_pyfunction!(delay_test))?;
    m.add_wrapped(wrap_pyfunction!(wait_for_py_coro))?;
    m.add_class::<EventCompleter>()?;

    Ok(())
}
