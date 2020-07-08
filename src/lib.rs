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

fn set_fut_result(loop_: PyObject, fut: PyObject, res: PyObject) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let sr = fut.getattr(py, "set_result")?;

    loop_.call_method1(py, "call_soon_threadsafe", (sr, res))?;

    Ok(())
}

#[pyfunction]
fn delay_test(delay: u64, result: PyObject) -> PyResult<PyObject> {
    let (fut, res_fut, loop_): (PyObject, PyObject, PyObject) = {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let asyncio = PyModule::import(py, "asyncio")?;
        let loop_ = asyncio.call0("get_running_loop")?;
        let fut: PyObject = loop_.call_method0("create_future")?.into();
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

    Ok(())
}
